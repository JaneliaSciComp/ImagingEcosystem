#!/bin/env perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Calc qw(Add_Delta_Days);
use Date::Manip qw(UnixDate);
use DBI;
use HTML::TableExtract;
use IO::File;
use JSON;
use LWP::Simple;
use LWP::UserAgent;
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
my %OCOLOR = (20 => '#294121',
              40 => '#5792BB',
              63 => '#33475F') ;
my @HOST_NUMBERS = ('',2,3,);

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
# General
my $Error_rate = '';
my @Unavailable_hosts = ();

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
Intake => "SELECT IFNULL(DATE(i.capture_date),DATE(NOW())),i.create_date,"
          . "DATEDIFF(i.create_date,IFNULL(i.capture_date,NOW())),"
          . "TIME_TO_SEC(TIMEDIFF(i.create_date,i.capture_date)),objective "
          . " FROM image i JOIN image_data_mv id ON (i.id=id.id) "
          . "WHERE DATEDIFF(NOW(),DATE(i.create_date)) <= ? AND i.name LIKE '%lsm'",
Indexing => "SELECT COUNT(1) FROM image_vw i JOIN image_property_vw ipd ON "
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
  my(%captured,%count,%sum);
  my $ar = $sth{Intake}->fetchall_arrayref();
  my $today = UnixDate("today","%Y-%m-%d");
  my $ago = sprintf '%4d-%02d-%02d',
                    Add_Delta_Days(split('-',$today),-$DELTA_DAYS);
  $count{all} = scalar @$ar;
  my (%bin1,%bin2);
  my %cbin = map {(sprintf '%02d',$_) => 0} (0..23);
  my %max = (all => 0,20 => 0,40 => 0,63 => 0);
  my %max_capture = map { $_ => 0 } qw(all 20 40 63);
  my %max_create = map { $_ => 0 } qw(all 20 40 63);
  my ($ct_acc,$ct_cnt,$max_ct,$min_ct) = ((0)x3,1e9);
  foreach (@$ar) {
    my $objective = '';
    $objective = '20' if ($_->[4] =~ /20[Xx]/);
    $objective = '40' if ($_->[4] =~ /40[Xx]/);
    $objective = '63' if ($_->[4] =~ /63[Xx]/);
    $count{$objective}++;
    # Capture date
    if ($_->[0] ge $ago) {
      $captured{all}++;
      $captured{$objective}++;
    }
    $bin1{$_->[0]}++;
    if ($_->[0] eq $today) {
      $max_capture{all}++;
      $max_capture{$objective}++;
    }
    # Create date
    $cbin{(split(/[ :]/,$_->[1]))[1]}++ unless (index($_->[1],$today));
    $_->[1] =~ s/ .+//;
    $bin2{$_->[1]}++;
    if ($_->[1] eq $today) {
      $max_create{all}++;
      $max_create{$objective}++;
    }
    $max{all} = $_->[2] if ($_->[2] > $max{all});
    $max{$objective} = $_->[2] if ($_->[2] > $max{$objective});
    $sum{all} += $_->[2];
    $sum{$objective} += $_->[2];
    # Images created today
    if (!index($_->[1],$today) && ($_->[2] >= 0) && $_->[3]) {
      $ct_acc += $_->[3];
      $ct_cnt++;
      $max_ct = $_->[3] if ($_->[3] > $max_ct);
      $min_ct = $_->[3] if ($_->[3] < $min_ct);
    }
  }
  # Histograms
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
  # Today
  my $clock = '';
  $clock = &generateClock(arrayref => [map {$cbin{$_}} sort keys %cbin],
                          content => 'intakeclock',
                          title => "LSM intake/hour",
                          color => ['#009900'],
                          text_color => 'white',
                          width => '270px',
                          height => '270px') if ($max_create{all});
  my %today = map { $_ => '' } qw(all 20 40 63);
  foreach (qw(all 20 40 63)) {
    next if (($_ ne 'all') && (!$max_capture{$_} && !$max_create{$_}));
    $today{$_} .= "Images captured: $max_capture{$_}<br>";
    $today{$_} .= "Images ingested: $max_create{$_}";
    if ($ct_cnt) {
      $today{$_} .= '<br>Capture &rarr; TMOG cycle time<br>';
      $today{$_} .= '&nbsp;&nbsp;Minimum: ' . &displayElapsed($min_ct/3600) . br;
      $today{$_} .= '&nbsp;&nbsp;Maximum: ' . &displayElapsed($max_ct/3600) . br;
      $today{$_} .= '&nbsp;&nbsp;Average: ' . &displayElapsed(($ct_acc/$ct_cnt)/3600) . br;
    }
  }
  # Check for images awaiting indexing
  $sth{Indexing}->execute();
  my $icount = $sth{Indexing}->fetchrow_array();
  $today{all} .= "<span style='color: #AB451D'><br>Images awaiting indexing: $icount</span>" if ($icount);
  $today{all} .= $clock;
  foreach (qw(all 20 40 63)) {
    $today{$_} = h3({style => 'text-align: center'},'Today').$today{$_} if ($today{$_});
  }
  # Last 30 days
  my %last = map { $_ => '' } qw(all 20 40 63);
  foreach (qw(all 20 40 63)) {
    my $title = '';
    unless (/all/) {
      $title = span({style => 'padding: 3px 5px 2px 5px; background-color: '
                              . $OCOLOR{$_} 
                              . '; color: #fff'},$_ . 'x objective') . br;
                             
    }
    $last{$_} = h3({style => 'text-align: center'},$title,
                   "Last $DELTA_DAYS days ($ago)")
                . "Images captured: $captured{$_}" . br
                . "Images ingested: $count{$_}" . br
                . "Capture &rarr; TMOG cycle time<br>"
                . "&nbsp;&nbsp;Average: "
                . &displayElapsed($sum{$_}/$count{$_},'d') . br
                . '&nbsp;&nbsp;Maximum: '
                . &displayElapsed($max{$_},'d');
  }
  my $intake = div({class => 'panel panel-primary'},
                   div({class => 'panel-heading'},
                       span({class => 'panel-heading;'},'Intake')),
                   div({class => 'panel-body'},
                       div({style => 'float: left'},
                           div({style => 'float: left; margin-right: 10px;'},
                               div({class => 'boxed'},$last{all},hr,$today{all})),
                           div({style => 'float: left'},$histogram1,br,$histogram2),
                           div({style => 'float: left; margin-left: 10px;'},
                               join('',map {div({class => 'boxed'},$last{$_},
                                                ($today{$_}) ? hr . $today{$_} : '')} qw(20 40 63))),
                   )))
               . div({style => 'clear: both;'},NBSP);
  my $pipeline = ($INTAKE) ? '' : &reportStatus();
  &printCurrentStatus();
  print $intake,$pipeline;
  if ($INTAKE) {
    print end_form,end_html;
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
  my $rvar = &getREST($CONFIG{url}.$CONFIG{query}{PipelineStatus}.'?hours=1440');
  foreach (@$rvar) {
    my $s = $_->{status};
    next unless ($s =~ /^(?:Complete|Error|Marked)/);
    (my $date = $_->{updatedDate}) =~ s/ .*//;
    next if (($date eq '2016-10-14') && ($s =~ /^Marked/));
    ($s eq 'Complete') ? $bin3{$date}++ : $bin4{$date}++;
  }
  my @bin3 = map { [$_,$bin3{$_}] } sort keys %bin3;
  my @bin4 = map { [$_,$bin4{$_}] } sort keys %bin4;
  my %bin5 = map { $_ => 1 } keys(%bin3);
  $bin5{$_} = 1 foreach (keys %bin3);
  my $hashref = {'_categories' => [sort keys %bin5],
                 Complete => [map { $bin3{$_}||0 } sort keys %bin5],
                 Error => [map { $bin4{$_}||0 } sort keys %bin5]
                };
  my $last_key = (sort keys %bin5)[-1];
  $Error_rate = sprintf '%.1f',($bin4{$last_key} / ($bin3{$last_key} + $bin4{$last_key})) * 100;
  my $width = 600;
  %PARMS = (text_color => '#fff', width => $width.'px',
            height => (sprintf '%d',$width*.6).'px');
  my $format = "this.x + '<br><b>' + this.series.name + ':</b> ' + this.y";
  my $histogram3 = &generateHistogram(hashref => $hashref,
                                      title => 'Completions/errors per day',
                                      subtitle => 'Last 60 days',
                                      content => 'unstackedchart',
                                      formatter => $format,
                                      color => ['#50b432','#cc6633'],
                                      yaxis_title => '# samples',
                                      %PARMS);
  my $histogram4 = &generateHistogram(hashref => $hashref,
                                      title => 'Completions/errors per day',
                                      subtitle => 'Last 60 days',
                                      content => 'stackedchart',
                                      formatter => $format,
                                      stacked => 1,
                                      color => ['#50b432','#cc6633'],
                                      yaxis_title => '# samples',
                                      %PARMS);
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
                                     width => '400px', height => '300px');
  my $export = &createExportFile(\@delta,'workstation_processing',
                                 ['Sample','User','Start date','Delta days']);
  # Processing samples
  my $processing = &getProcessingStats();
  # Render
  $disposition{Null} = 'In process';
  my $pipeline = div({style => 'float: left'},
                     div({style => 'float: left'},
                         table({id => 'stats',class => 'tablesorter standard'},
                               thead(Tr(th(['Disposition','Status','Count','%']))),
                               tbody(map {Tr(td([$disposition{$_},$_,&commify($count{$_}),
                                                 sprintf '%.2f%%',$count{$_}/$total*100]))}
                                    sort keys %count)),
                         $donut1),
                     div({style => 'float: left',align => 'center'},$chart),br,
                     div({style => 'clear: both;'},NBSP),
                     div({style => 'float: left;'},
                         div({style => 'float: left;padding-right: 10px;'},$pie2),
                         div({style => 'float: left;padding-right: 10px;'},$pie3,br,$export),
                         div({style => 'float: left;padding-right: 10px;'},
                             span({style => 'font-family: "Lucida Grande",color:#bbc;font-size:20px;fill:#bbc;'},
                                  'Server status'),(br)x2,$processing),
                        ),br,
                     div({style => 'float: left'},
                         div({style => 'float: left'},$histogram3),
                         div({style => 'float: left'},$histogram4)
                        ));
  return (div({class => 'panel panel-primary'},
             div({class => 'panel-heading'},
                 span({class => 'panel-heading;'},
                      (($MONGO) ? img({src => '/images/mongodb.png'}) : ''),
                      'Workstation pipeline')),
             div({class => 'panel-body'},$pipeline))
          . div({style => 'clear: both;'},NBSP)
          . end_form . &sessionFooter($Session) . end_html);
}


sub printCurrentStatus
{
  return unless ($Error_rate);
  my $total_hosts = scalar @HOST_NUMBERS;
  my $active_hosts = $total_hosts - scalar(@Unavailable_hosts);
  my $err_msg;
  if ($active_hosts != $total_hosts) {
    $err_msg = ' (unavailable servers: ' . join(', ',@Unavailable_hosts) . ')';
  }
  my $err_style = 'lime';
  $err_style = 'red' if ($Error_rate > 5);
  $err_style = "color: $err_style";
  my $srv_style = 'lime';
  $srv_style = 'red' unless ($active_hosts == $total_hosts);
  $srv_style = "color: $srv_style";
  print div({class => 'panel panel-primary'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},'Current status')),
            div({class => 'panel-body',style => 'font-size: 18pt'},
                'Error rate: ',
                span({style => $err_style},$Error_rate.'%'),(NBSP)x10,
                'Available servers: ',
                span({style => $srv_style},
                     (sprintf '%d/%d',$active_hosts,$total_hosts),$err_msg)
               )),
           div({style => 'clear: both;'},NBSP);
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


sub getProcessingStats
{
  my $ua = LWP::UserAgent->new;
  my $suffix = ':8180/jmx-console/HtmlAdaptor?action=inspectMBean&name=jboss.mq.destination%3Aservice%3DQueue%2Cname%3DsamplePipelineLauncher';
  my %hash;
  foreach my $hostnum (@HOST_NUMBERS) {
    my $url = 'http://jacs-data' . $hostnum . $suffix;
    my $request = HTTP::Request->new(GET => $url);
    my $response = $ua->request($request);
    if ($response->code == 200) {
      my $content = $response->content;
      my $te = HTML::TableExtract->new(headers => [qw(Name Type Value Access Description)]);
      $te->parse($content);
      foreach my $ts ($te->tables) {
        foreach my $row ($ts->rows) {
          $hash{'jacs-data'.$hostnum}{$row->[0]} = $row->[2];
        }
      }
    }
    else {
      push @Unavailable_hosts,'jacs-data'.$hostnum;
    }
  }
  my @row = ();
  my @sum = (span({style => 'font-weight: bold'},'TOTAL'));
  foreach my $hostnum (@HOST_NUMBERS) {
    my $host = 'jacs-data' . $hostnum;
    (my $queued = ($hash{$host}{QueueDepth} || '-')) =~ s/^\s+//;
    chomp($queued);
    (my $on_queue = ($hash{$host}{InProcessMessageCount} || '-')) =~ s/^\s+//;
    chomp($on_queue);
    push @row,[$host,$queued,$on_queue];
    $sum[1] += $queued;
    $sum[2] += $on_queue;
  }
  table({id => 'proc', class => 'tablesorter standard'},
        thead(Tr(th([qw(Server Queued),'On cluster']))),
        tbody(map {Tr(td($_->[0]),td({style => 'text-align: center'},[@$_[1..2]]))} @row),
        tfoot(Tr(td($sum[0]),td({style => 'text-align: center'},[@sum[1..2]])))
       );
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
