#!/bin/env perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Calc qw(Add_Delta_Days);
use Date::Manip qw(UnixDate);
use HTML::TableExtract;
use IO::File;
use JSON;
use LWP::Simple qw(get);
use LWP::UserAgent;
use POSIX qw(strftime);
use Time::Local;
use XML::Simple;
use JFRC::Utils::Web qw(:all);
use JFRC::Highcharts qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant LOG_PATH => '/groups/scicomp/informatics/logs/';
my $BASE = "/var/www/html/output/";
my %CONFIG = (config => {url => 'http://config.int.janelia.org/'});

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Workstation dashboard';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $MEASUREMENT_DAYS = param('days') || 30;
my $MEASUREMENT_HOURS = $MEASUREMENT_DAYS * 24;
my %OCOLOR = (20 => '#294121',
              40 => '#5792BB',
              63 => '#33475F') ;
my @HOST_NUMBERS = ('',2..8);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
my %PARMS;
# Web
our ($USERID,$USERNAME);
my $INTAKE = 0;
my $Session;
# General
my ($Capture_per_day,$Completed,$CT_cycle_time,$DC_cycle_time,$Errored,
    $Error_rate,$Intake_per_day) = ('')x7;
my($Total_queued,$Total_on_cluster) = (0)x2;
my @Unavailable_hosts = ();
my %status_count;

# ****************************************************************************
$INTAKE = param('intake');
unless ($INTAKE) {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
}


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayDashboard();
# We're done!
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub getREST
{
  my($server,$endpoint) = @_;
  my $url = join('',$CONFIG{$server}{url},$endpoint);
  my $response = get $url;
  return() unless ($response && length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $url<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}


sub initializeProgram
{
  # Get general REST config
  my $rvar = &getREST('config','config/rest_services');
  %CONFIG =  %{$rvar->{config}};
}


sub displayDashboard
{
  my $width = param('width') || 530;
  %PARMS = (text_color => '#fff', width => $width.'px',
            height => (sprintf '%d',$width*.6).'px');
  &printHeader();
  my $pipeline = ($INTAKE) ? '' : &reportStatus();
  # Intake
  my(%captured,%count,%sum);
  my $ar;
  my $rvar = &getREST('sage',"images_tmogged_since/days/$MEASUREMENT_DAYS");
  foreach (@{$rvar->{images}}) {
    $_->{'capture_date'} =~ s/ .+//;
    push @$ar,[@{$_}{qw(capture_date create_date capture_tmog_cycle_days capture_tmog_cycle_sec objective)}];
  }
  my $today = UnixDate("today","%Y-%m-%d");
  my $ago = sprintf '%4d-%02d-%02d',
                    Add_Delta_Days(split('-',$today),-$MEASUREMENT_DAYS);
  $count{all} = scalar @$ar;
  my (%bin1,%bin2);
  my %cbin = map {(sprintf '%02d',$_) => 0} (0..23);
  my %max = map {$_ => 0} qw(all 20 40 63);
  my %min = map {$_ => 1e9} qw(all 20 40 63);
  my %max_capture = map { $_ => 0 } qw(all 20 40 63);
  my %max_create = map { $_ => 0 } qw(all 20 40 63);
  my (%ct_acc,%ct_cnt);
  my %max_ct = map {$_ => 0} qw(all 20 40 63 unknown);
  my %min_ct = map {$_ => 1e9} qw(all 20 40 63 unknown);
  my $img_acc = 0;
  foreach (@$ar) {
    $_->[4] ||= '';
    my $objective = '';
    $objective = '20' if ($_->[4] =~ /20[Xx]/);
    $objective = '40' if ($_->[4] =~ /40[Xx]/);
    $objective = '63' if ($_->[4] =~ /63[Xx]/);
    $objective = 'unknown' unless ($objective);
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
    $img_acc++;
    # Create date
    $cbin{(split(/[ :]/,$_->[1]))[1]}++ unless (index($_->[1],$today));
    $_->[1] =~ s/ .+//;
    $bin2{$_->[1]}++;
    if ($_->[1] eq $today) {
      $max_create{all}++;
      $max_create{$objective}++;
    }
    $_->[2] ||= 0;
    $_->[3] ||= 0;
    $_->[2] = 0 if ($_->[2] < 0);
    $_->[3] = 0 if ($_->[3] < 0);
    $min{all} = $_->[3] if ($_->[3] < $min{all});
    $min{$objective} = $_->[3] if (defined($min{$objective}) && $_->[3] < $min{$objective});
    $max{all} = $_->[2] if ($_->[2] > $max{all});
    $max{$objective} = $_->[2] if (defined($max{$objective}) && $_->[2] > $max{$objective});
    $sum{all} += $_->[2];
    $sum{$objective} += $_->[2];
    # Images created today
    if (!index($_->[1],$today) && ($_->[2] >= 0) && $_->[3]) {
      foreach my $o ('all',$objective) {
        $ct_acc{$o} += $_->[3];
        $ct_cnt{$o}++;
        $max_ct{$o} = $_->[3] if ($_->[3] > $max_ct{$o});
        $min_ct{$o} = $_->[3] if (($_->[3] > 0) && ($_->[3] < $min_ct{$o}));
      }
    }
  }
  # Histograms
  &fillDates(\%bin2);
  $Capture_per_day = $img_acc / scalar(keys %bin1);
  $Intake_per_day = $img_acc / scalar(keys %bin2);
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
    if ($ct_cnt{$_}) {
      $today{$_} .= '<br>Capture &rarr; TMOG cycle time<br>';
      $today{$_} .= '&nbsp;&nbsp;' . &displayElapsed($min_ct{$_}/3600) . ' - '
                    . &displayElapsed($max_ct{$_}/3600) . br;
      $today{$_} .= '&nbsp;&nbsp;Average: ' . &displayElapsed(($ct_acc{$_}/$ct_cnt{$_})/3600) . br;
    }
  }
  # Check for images awaiting indexing
  $rvar = &getREST('sage',"unindexed_images");
  my $icount = scalar(@{$rvar->{images}}) || 0;
  $today{all} .= "<span style='color: #fff; background-color: #AB451D'><br>Images awaiting indexing: $icount</span>" if ($icount);
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
    $last{$_} = '';
    $CT_cycle_time = &displayElapsed($sum{$_}/$count{$_},'d')
      unless ($CT_cycle_time);
    $last{$_} = h3({style => 'text-align: center'},$title,
                   "Last $MEASUREMENT_DAYS days ($ago)")
                . "Images captured: $captured{$_}" . br
                . "Images ingested: $count{$_}" . br
                . "Capture &rarr; TMOG cycle time<br>"
                . "&nbsp;&nbsp;" . &displayElapsed($min{$_}/3600) . ' - '
                . &displayElapsed($max{$_},'d') . br
                . "&nbsp;&nbsp;Average: "
                . &displayElapsed($sum{$_}/$count{$_},'d')
      if ($count{$_});
  }
  my $objective_boxes = '';
  foreach my $o qw(20 40 63) {
    next unless ($last{$o});
    $objective_boxes .= div({class => 'boxed'.$o},$last{$o},
                            ($today{$o}) ? hr . $today{$o} : '');
  }
  my $intake = div({class => 'panel panel-primary'},
                   div({class => 'panel-heading'},
                       span({class => 'panel-heading;'},'Intake')),
                   div({class => 'panel-body'},
                       div({class => 'left'},
                           div({class => 'left10'},
                               div({class => 'boxed'},$last{all},hr,$today{all})),
                           div({class => 'left10'},$histogram1,br,$histogram2),
                           div({class => 'left'},$objective_boxes))))
               . div({style => 'clear: both;'},NBSP);
  &printCurrentStatus();
  print $intake,$pipeline;
  if ($INTAKE) {
    print end_form,end_html;
  }
}


sub reportStatus
{
  # Read status counts from workstation_status.log
  my $file =  LOG_PATH . 'workstation_status.log';
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
    next if ($status =~ /(?:Blocked|Complete|Retired|Desync|Marked|New|Null|Queued)/);
    $chash{$date}{$status} = 1*$count;
  }
  $stream->close();
  my (%count,%donut,%piec,%piei);
  my $total = 0;
  my $ar;
  my $rvar = &getREST('jacs',$CONFIG{'jacs'}{query}{SampleStatus});
  foreach (@$rvar) {
    $_->{'_id'} ||= 'Null';
    push @$ar,[@{$_}{qw(_id count)}];
  }
  foreach (@$ar) {
    $status_count{$_->[0]} = $_->[1];
    $total += $_->[1];
    next if ($_->[0] eq 'Null');
    ($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? $piec{$_->[0]} = $_->[1]
                                                : $piei{$_->[0]} = $_->[1];
    $donut{($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? 'Complete' : 'In process'} += $_->[1];
  }
  my (%bin3,%bin4);
  $rvar = &getREST('jacs',$CONFIG{'jacs'}{query}{PipelineStatus}
                      . '?hours=' . $MEASUREMENT_HOURS);
  my ($pipeline_acc,$samples,$successful) = (0)x3;
  foreach (@$rvar) {
    my $s = $_->{status};
    next unless ($s =~ /^(?:Complete|Error|Marked)/);
    (my $date = $_->{updatedDate}) =~ s/ .*//;
    next if (($date eq '2016-10-14') && ($s =~ /^Marked/)); #PLUG
    $samples++;
    ($s eq 'Complete') ? $bin3{$date}++ : $bin4{$date}++;
    if ($s eq 'Complete') {
      $successful++;
      $pipeline_acc += $_->{pipelineTime};
    }
  }
  $DC_cycle_time = sprintf '%.2f',$pipeline_acc/$successful/24 if ($successful);
  my @bin3 = map { [$_,$bin3{$_}] } sort keys %bin3;
  my @bin4 = map { [$_,$bin4{$_}] } sort keys %bin4;
  my %bin_days = map { $_ => 1 } keys(%bin3);
  my %bin5;
  foreach (sort keys %bin_days) {
    if (exists($bin3{$_}) && exists($bin4{$_})) {
      $bin5{$_}{'Error rate %'} = 0 + sprintf '%.2f',($bin4{$_} / ($bin3{$_}+$bin4{$_}) * 100);
    }
    else {
      $bin5{$_}{'Error rate %'} = 0;
    }
  }
  $bin_days{$_} = 1 foreach (keys %bin3);
  my $hashref = {'_categories' => [sort keys %bin_days],
                 Complete => [map { $bin3{$_}||0 } sort keys %bin_days],
                 Error => [map { $bin4{$_}||0 } sort keys %bin_days]};
  my $last_key = (sort keys %bin_days)[-1];
  $Completed = $bin3{$last_key} + $bin4{$last_key};
  $Errored = $bin4{$last_key} || 0;
  $Error_rate = sprintf '%.1f',($Errored / $Completed) * 100;
  my $width = 600;
  %PARMS = (subtitle => "$samples samples over the last $MEASUREMENT_DAYS days",
            text_color => '#fff', width => $width.'px',
            height => (sprintf '%d',$width*.6).'px');
  my $format = "this.x + '<br><b>' + this.series.name + ':</b> ' + this.y";
  my $histogram3 = &generateHistogram(hashref => $hashref,
                                      title => 'Completions/errors per day',
                                      content => 'unstackedchart',
                                      formatter => $format,
                                      color => ['#50b432','#cc6633'],
                                      yaxis_title => '# samples',
                                      %PARMS);
  my $histogram4 = &generateHistogram(hashref => $hashref,
                                      title => 'Completions/errors per day',
                                      content => 'stackedchart',
                                      formatter => $format,
                                      stacked => 1,
                                      color => ['#50b432','#cc6633'],
                                      yaxis_title => '# samples',
                                      %PARMS);
  my $line2 = &generateSimpleLineChart(hashref => \%bin5,
                                       title => 'Error rate',
                                       color => ['#cc6633'],
                                       %PARMS);
  my @color = ('#ff6666','#6666ff','#ff66ff','#66ffff','#ccffcc');
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
  my $line1 = &generateSimpleLineChart(hashref => \%chash,
                                       title => 'Sample status history (in process)',
                                       subtitle => "$first_date - $last_date",
                                       content => 'status',
                                       background_color => '#222',
                                       color => \@color,
                                       text_color => '#bbc',
                                       );
  # Age of processing samples
  @$ar = ();
  $rvar = &getREST('jacs',$CONFIG{'jacs'}{query}{SampleAging});
  # {"name":"20160107_31_A2","ownerKey":"group:flylight","updatedDate":1454355394000,"status":"Complete"}
  foreach (@$rvar) {
    push @$ar,[$_->{name},$_->{ownerKey},$_->{updatedDate}];
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
  # Render
  $disposition{Null} = 'In process';
  my $pipeline = div({class => 'left'},
                     div({class => 'left'},
                         table({id => 'stats',class => 'tablesorter standard'},
                               thead(Tr(th(['Disposition','Status','Count','%']))),
                               tbody(map {Tr(td([$disposition{$_},$_,&commify($status_count{$_}),
                                                 sprintf '%.2f%%',$status_count{$_}/$total*100]))}
                                    sort keys %status_count)),
                         $donut1),
                     div({class => 'left',align => 'center'},$line1),br,
                     div({style => 'clear: both;'},NBSP),
                     div({class => 'left'},
                         div({class => 'left10'},$pie1),
                         div({class => 'left10'},$pie2),
                         div({class => 'left10'},$pie3,br,$export),
                        ),br,
                     div({class => 'left'},
                         div({class => 'left'},$line2),
                         div({class => 'left'},$histogram4)
                        ));
  return (div({class => 'panel panel-primary'},
             div({class => 'panel-heading'},
                 span({class => 'panel-heading;'},
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
  if ($Error_rate > 10) {
    $err_style = 'red';
  }
  elsif ($Error_rate > 5) {
    $err_style = 'orange';
  }
  $err_style = "color: $err_style";
  my $srv_style = 'lime';
  $srv_style = 'red' unless ($active_hosts == $total_hosts);
  $srv_style = "color: $srv_style";
  my $ct = '';
  $ct = 'Average cycle time:' . br if ($CT_cycle_time || $DC_cycle_time);
  $ct .= span({style => "color: ".&getColor($CT_cycle_time)},
              $CT_cycle_time) . ' days'
         . span({style => 'font-size: 10pt'},
                " (Capture &rarr; TMOG over $MEASUREMENT_DAYS days)") . br;
  if ($DC_cycle_time) {
    $ct .= span({style => "color: ".&getColor($DC_cycle_time)},
                $DC_cycle_time) . ' days'
           . span({style => 'font-size: 10pt'},
                  " (Discovery &rarr; Completion over $MEASUREMENT_DAYS days)");
  }
  my $it = '';
  $it .= sprintf "Capture: %d<br>",$Capture_per_day if ($Capture_per_day);
  $it .= sprintf "Intake: %d<br>",$Intake_per_day if ($Intake_per_day);
  if ($it) {
    $it = 'Average LSMs per day:' . br
          . div({style => 'font-size: 16pt; padding-left: 10px;'},$it);
  }
  my $scheduled = '';
  if ($a = $status_count{Scheduled}) {
    $scheduled = sprintf "%d sample%s scheduled but not queued<br>to a JACS server",$a,($a == 1) ? '' : 's';
    $scheduled = div({class => "panel panel-danger"},
                     div({class => "panel-body"},
                         span({style => 'font-size: 10pt;color: #f33;'},$scheduled)));
  }
  my $processing_stats = &getProcessingStats();
  my $panel = 'primary';
  my $current = 'Current status';
  if ($scheduled && !$Total_queued && !$Total_on_cluster) {
    $panel = 'danger';
    $current = span({style => 'font-size: 20pt;color: #f33;'},'Pipeline is shut down');
  }
  print div({class => "panel panel-$panel"},
             div({class => 'panel-heading'},
                span({class => 'panel-heading;'},$current)),
            div({class => 'panel-body',style => 'font-size: 18pt'},
                div({class => 'left'},
                    div({class=> 'left30'},
                        'Error rate: ',
                        span({style => $err_style},$Error_rate.'%'),
                        span({style => 'font-size: 11pt'},"($Errored/$Completed samples)")),
                div({class=> 'left30'},
                    $scheduled,
                    'Available JACS servers: ',
                    span({style => $srv_style},
                         (sprintf '%d/%d',$active_hosts,$total_hosts),$err_msg),
                    $processing_stats),
                div({class=> 'left30'},$ct,br,$it)
               ))),
           div({style => 'clear: both;'},NBSP);
}


sub getColor
{
  my($num) = shift;
  $num =~ s/ .+//;
  my $ct_style = 'lime';
  if ($num > 2) {
    $ct_style = 'red';
  }
  elsif ($num > 1) {
    $ct_style = 'orange';
  }
  return($ct_style);
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
  ($Total_queued,$Total_on_cluster) = @sum[1..2];
  table({id => 'proc', class => 'tablesorter standard'},
        thead(Tr(th([qw(Server Queued),'On cluster']))),
        tbody(map {Tr(td(a({href => 'http://' . $_->[0] . $suffix,
                            target => '_blank'},$_->[0])),
                      td({style => 'text-align: center'},[@$_[1..2]]))} @row),
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
                     'jquery/jquery.tablesorter','tablesorter',$PROGRAM);
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
