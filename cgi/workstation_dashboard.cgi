#!/bin/env perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Calc qw(Add_Delta_Days);
use Date::Manip qw(UnixDate);
use DBI;
use IO::File;
use JSON;
use LWP::Simple;
use POSIX qw(strftime);
use Time::Local;
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);
use JFRC::Highcharts qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';
my $BASE = "/var/www/html/output/";

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Workstation dashboard';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my %CONFIG;
use constant NBSP => '&nbsp;';
my $DELTA_DAYS = 30;

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
my %PARMS;
# Web
our ($USERID,$USERNAME);
my ($INTAKE,$MONGO) = (0)x2;
my $Session;
# Database
our ($dbh,$dbhs);

# ****************************************************************************
$INTAKE = param('intake');
unless ($INTAKE) {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
}
my %sth = (
Intake => "SELECT IFNULL(DATE(capture_date),DATE(NOW())),create_date,"
          . "DATEDIFF(create_date,IFNULL(capture_date,NOW())),TIME_TO_SEC(TIMEDIFF(create_date,capture_date)) FROM image "
          . "WHERE DATEDIFF(NOW(),DATE(create_date)) <= ? AND name like '%lsm'",
Indexing => "SELECT COUNT(1)  FROM image_vw i JOIN image_property_vw ipd ON "
            . "(i.id=ipd.image_id AND ipd.type='data_set') WHERE "
            . "i.family NOT LIKE 'simpson%' AND i.id NOT IN "
            . "(SELECT image_id FROM image_property_vw WHERE type='bits_per_sample')",
WS_Status => "SELECT value,COUNT(1) FROM entityData WHERE entity_att='Status' GROUP BY 1",
WS_Aging => "SELECT name,e.owner_key,ed.updated_date FROM entity e "
            . "JOIN entityData ed ON (e.id=ed.parent_entity_id) WHERE "
            . "entity_att='Status' AND value='Processing' ORDER BY 3",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayDashboard();
# We're done!
if ($dbh || $dbhs) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
  $dbhs->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Get WS REST config
  my $file = DATA_PATH . 'workstation_ng.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %CONFIG = %$hr;
  $MONGO = (param('mongo')) || ('mongo' eq $CONFIG{data_source});

  # Connect to databases
  &dbConnect(\$dbh,'workstation');
  &dbConnect(\$dbhs,'sage');
  foreach (keys %sth) {
    if (/^WS/) {
      (my $n = $_) =~ s/WS_//;
      $sth{$n} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
    }
    else {
      $sth{$_} = $dbhs->prepare($sth{$_}) || &terminateProgram($dbhs->errstr);
    }
  }
}


sub displayDashboard
{
  my $width = param('width') || 530;
  %PARMS = (text_color => '#fff', width => $width.'px',
            height => (sprintf '%d',$width*.6).'px');
  &printHeader();
  # Intake
  $sth{Intake}->execute($DELTA_DAYS);
  my($captured,$count,$sum) = (0)x3;
  my $ar = $sth{Intake}->fetchall_arrayref();
  $count = scalar @$ar;
  my (%bin1,%bin2);
  my %cbin = map {(sprintf '%02d',$_) => 0} (0..23);
  my $max = 0;
  my $today = UnixDate("today","%Y-%m-%d");
  my ($ct_acc,$ct_cnt,$max_ct,$min_ct) = ((0)x3,1e9);
  my $ago = sprintf '%4d-%02d-%02d',Add_Delta_Days(split('-',$today),-$DELTA_DAYS);
  foreach (@$ar) {
    $captured++ if ($_->[0] ge $ago);
    $bin1{$_->[0]}++;
    $cbin{(split(/[ :]/,$_->[1]))[1]}++ unless (index($_->[1],$today));
    $_->[1] =~ s/ .+//;
    $bin2{$_->[1]}++;
    $max = $_->[2] if ($_->[2] > $max);
    $sum += $_->[2];
    if (!index($_->[1],$today) && ($_->[2] >= 0) && $_->[3]) {
      $ct_acc += $_->[3];
      $ct_cnt++;
      $max_ct = $_->[3] if ($_->[3] > $max_ct);
      $min_ct = $_->[3] if ($_->[3] < $min_ct);
    }
  }
  my $max_capture = $bin1{$today} || 0;
  my $max_create = $bin2{$today} || 0;
  &fillDates(\%bin2);
  my @bin1 = map { [$_,$bin1{$_}] } sort keys %bin1;
  my @bin2 = map { [$_,$bin2{$_}] } sort keys %bin2;
  my $histogram1 = &generateHistogram(arrayref => \@bin1,
                                      title => 'LSM file capture per day',
                                      content => 'capture',
                                      yaxis_title => '# files',
                                      color => '#66f',%PARMS);
  my $histogram2 = &generateHistogram(arrayref => \@bin2,
                                      title => 'LSM file intake per day',
                                      content => 'intake',
                                      yaxis_title => '# files',
                                      color => '#6f6',%PARMS);
  my $clock = '';
  $clock = &generateClock(arrayref => [map {$cbin{$_}} sort keys %cbin],
                          content => 'intakeclock',
                          title => "LSM intake/hour",
                          color => ['#009900'],
                          text_color => 'white',
                          width => '270px',
                          height => '270px') if ($max_create);
  my $today_status = '';
  $today_status .= "Images captured: $max_capture<br>";
  $today_status .= "Images ingested: $max_create";
  if ($ct_cnt) {
    $today_status .= '<br>Capture &rarr; TMOG cycle time<br>';
    $today_status .= '&nbsp;&nbsp;Minimum: ' . &displayElapsed($min_ct/3600) . br;
    $today_status .= '&nbsp;&nbsp;Maximum: ' . &displayElapsed($max_ct/3600) . br;
    $today_status .= '&nbsp;&nbsp;Average: ' . &displayElapsed(($ct_acc/$ct_cnt)/3600) . br;
  }
  $sth{Indexing}->execute();
  my $icount = $sth{Indexing}->fetchrow_array();
  $today_status .= "<span style='color: #AB451D'><br>Images awaiting indexing: $icount</span>" if ($icount);
  $today_status .= $clock;
  $today_status = div({class => 'boxed'},h3({style => 'text-align: center'},'Today'),$today_status) if ($today_status);
  my $last_days = div({class => 'boxed'},h3({style => 'text-align: center'},"Last $DELTA_DAYS days ($ago)"),
                      "Images captured: $captured",br,
                      "Images ingested: $count",br,
                      "Capture &rarr; TMOG cycle time<br>",
                      "&nbsp;&nbsp;Average: ",&displayElapsed($sum/$count,'d'),br,
                      '&nbsp;&nbsp;Maximum: ',&displayElapsed($max,'d'));
  print div({class => 'panel panel-primary'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},'Intake')),
            div({class => 'panel-body'},
                div({style => 'float: left'},
                    div({style => 'float: left; margin-right: 10px;'},
                        $last_days,$today_status
                       ),
                    div({style => 'float: left'},$histogram1),
                    div({style => 'float: left'},$histogram2))
               )),
        div({style => 'clear: both;'},NBSP);
  if ($INTAKE) {
    print end_form,end_html;
  }
  else {
    &reportStatus();
  }
}


sub reportStatus
{
  # Read status counts from workstation_status.log
  my $file =  DATA_PATH . 'workstation_status.log';
  my $stream = new IO::File $file,"<"
    || &terminateProgram("Can't open $file ($!)");
  my (%chash,%disposition);
  my ($first_date,$last_date) = ('')x2;
  while (defined(my $line = $stream->getline)) {
    chomp($line);
    my($date,$status,$count) = split(/\t/,$line);
    $first_date = $date unless ($first_date);
    if ($last_date ne $date) {
      $last_date = $date;
    }
    $disposition{$status} = ($status =~ /(?:Blocked|Complete|Retired)/)
                            ? 'Complete' : 'In process';
    next if ($status =~ /(?:Blocked|Complete|Retired)/);
    $chash{$date}{$status} = 1*$count;
  }
  $stream->close();
  my (%count,%donut,%piec,%piei);
  my $total = 0;
  my $ar;
  if ($MONGO) {
    my $rvar = &getREST($CONFIG{url}.$CONFIG{query}{SampleStatus});
    foreach (@$rvar) {
      $_->{'_id'} ||= 'Null';
      push @$ar,[@{$_}{qw(_id count)}];
    }
  }
  else {
    $sth{Status}->execute();
    $ar = $sth{Status}->fetchall_arrayref();
  }
  foreach (@$ar) {
    $count{$_->[0]} = $_->[1];
    $total += $_->[1];
    ($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? $piec{$_->[0]} = $_->[1]
                                                : $piei{$_->[0]} = $_->[1];
    $donut{($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? 'Complete' : 'In process'} += $_->[1];
  }
  my (%bin3,%bin4);
  my $rvar = &getREST($CONFIG{url}.$CONFIG{query}{PipelineStatus}.'?hours=720');
  foreach (@$rvar) {
    my $s = $_->{status};
    next unless ($s =~ /(?:Complete|Error)/);
    (my $date = $_->{updatedDate}) =~ s/ .*//;
    ($s eq 'Error') ? $bin4{$date}++ : $bin3{$date}++;
  }
  my @bin3 = map { [$_,$bin3{$_}] } sort keys %bin3;
  my @bin4 = map { [$_,$bin4{$_}] } sort keys %bin4;
  my $histogram3 = &generateHistogram(arrayref => \@bin3,
                                      title => 'Sample completion per day',
                                      content => 'completion',
                                      yaxis_title => '# samples',
                                      color => '#6f6',%PARMS);
  my $histogram4 = &generateHistogram(arrayref => \@bin4,
                                      title => 'Sample errors per day',
                                      content => 'errors',
                                      yaxis_title => '# samples',
                                      color => '#f66',%PARMS);
  my @color = ('#ff6666','#6666ff','#ff66ff','#66ffff');
  my $donut1 = &generateHalfDonutChart(hashref => \%donut,
                                       title => 'Disposition',
                                       content => 'disposition',
                                       color => ['#50b432','#cc6633'],
                                       text_color => 'white',
                                       label_format => "this.point.name",
                                       width => '400px', height => '300px',
                                      );
  my $pie1 = &generateSimplePieChart(hashref => \%piec,
                                     title => 'Completed samples',
                                     content => 'pie1',
                                     color => ['#4444ff','#44ff44','#ff9900'],
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '400px', height => '300px',
                                    );
  my $pie2 = &generateSimplePieChart(hashref => \%piei,
                                     title => 'Samples in process',
                                     content => 'pie2',
                                     color => \@color,
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '400px', height => '300px',
                                    );
  my $chart = &generateSimpleLineChart(hashref => \%chash,
                                       title => 'Sample status history (in process)',
                                       subtitle => "$first_date - $last_date",
                                       content => 'status',
                                       color => \@color,
                                       text_color => '#bbc',
                                       );
  # Age of processing samples
  if ($MONGO) {
    @$ar = ();
    my $rest = $CONFIG{url}.$CONFIG{query}{SampleAging};
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    # {"name":"20160107_31_A2","ownerKey":"group:flylight","updatedDate":1454355394000,"status":"Complete"}
    foreach (@$rvar) {
      push @$ar,[$_->{name},$_->{ownerKey},$_->{updatedDate}];
    }
  }
  else {
    $sth{Aging}->execute();
    $ar = $sth{Aging}->fetchall_arrayref();
  }
  my @delta;
  %piec = ();
  my $now = time;
  foreach (@$ar) {
    my @f = split(/[-: T]/,$_->[-1]);
    $f[1]--;
    my $then = timelocal(reverse @f);
    my $delta_hours = ($now - $then) / 3600;
    if ($delta_hours < 48) {
      $piec{'< 2 days'}++;
    }
    elsif ($delta_hours < 168) {
      $piec{'2 days - 1 week'}++;
    }
    elsif ($delta_hours < 720) {
      $piec{'1 week - 1 month'}++;
    }
    else {
      $piec{'> 1 month'}++;
    }
    $_->[1] =~ s/.+://;
    push @delta,[@$_,sprintf '%.1f',$delta_hours/24];
  }
  my @pcolor;
  push @pcolor,'#cc9900' if (exists $piec{'1 week - 1 month'});
  push @pcolor,'#cccc33' if (exists $piec{'2 days - 1 week'});
  push @pcolor,'#44cc44' if (exists $piec{'< 2 days'});
  push @pcolor,'#cc4444' if (exists $piec{'> 1 month'});
  my $pie3 = &generateSimplePieChart(hashref => \%piec,
                                     title => 'Age of Processing samples',
                                     content => 'pie3',
                                     color => \@pcolor,
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '600px', height => '400px');
  my $export = &createExportFile(\@delta,'workstation_processing',
                                 ['Sample','User','Start date','Delta days']);
  # Render
  $disposition{Null} = 'In process';
  my $pipeline = div({style => 'float: left'},
                     div({style => 'float: left'},
                         table({id => 'stats',class => 'tablesorter standard'},
                               thead(Tr(th(['Disposition','Status','Count','%']))),
                               tbody(map {Tr(td([$disposition{$_},$_,&commify($count{$_}),
                                                 sprintf '%.2f%%',$count{$_}/$total*100]))}
                                    sort keys %count)),
                         $donut1,br,$pie1,br,$pie2),
                     div({style => 'float: left',align => 'center'},$chart,br,$pie3,$export),br,
                     div({style => 'float: left'},
                         div({style => 'float: left'},$histogram3),
                         div({style => 'float: left'},$histogram4))
                    );
  print div({class => 'panel panel-primary'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},
                     (($MONGO) ? img({src => '/images/mongodb.png'}) : ''),
                     'Workstation pipeline')),
            div({class => 'panel-body'},$pipeline)),
        div({style => 'clear: both;'},NBSP);
  print end_form,&sessionFooter($Session),end_html;
}


sub getREST
{
  my($rest) = shift;
  my $response = get $rest;
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}


sub fillDates
{
  my $hr = shift;
  my @dates = sort keys %$hr;
  for (my $di=0; $di < $#dates; $di++) {
    my $next = $dates[$di+1];
    my $expected = sprintf '%4d-%02d-%02d',Add_Delta_Days(split('-',$dates[$di]),1);
    while ($next ne $expected) {
      $hr->{$expected} = 0;
      $expected = sprintf '%4d-%02d-%02d',Add_Delta_Days(split('-',$expected),1);
    }
  }
}


sub displayElapsed
{
  my($num,$unit) = @_;
  $unit ||= '';
  if ($unit eq 'd') {
    $num = ($num <= 1) ? sprintf('%.2f hours',$num/24)
                       : sprintf('%.2f days',$num);
  }
  else {
    $num = ($num < 24) ? sprintf('%.2f hours',$num)
                       : sprintf('%.2f days',$num/24);
  }
  $num =~ s/\.00//;
  return($num);
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d_%H%M%S",localtime)
                 . "$suffix.xls";
  my $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach (@$ar) {
    my @l = @$_;
    print $handle join("\t",@l) . "\n";
  }
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export Processing samples");
  return($link);
}


sub commify
{
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('highcharts-4.0.1/highcharts',
                     'highcharts-4.0.1/highcharts-more',
                     'jquery/jquery.tablesorter','tablesorter');
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1);
  my %load = (load => 'tableInitialize();');
  $load{load} .= " $onload" if ($onload);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now',
                        %load),
        start_multipart_form;
}
