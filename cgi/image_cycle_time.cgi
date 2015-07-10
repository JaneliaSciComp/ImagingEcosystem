#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Manip qw(UnixDate);
use DBI;
use Getopt::Long;
use IO::File;
use POSIX qw(ceil strftime);
use Statistics::Basic qw(:all);
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Slime qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Image processing cycle time';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";
# Highcharts
my $COLORS = '';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our ($dbh,$dbhw);
# General
my %summary;
my ($CHART,$MEASUREMENT,$MODE,$UNIT);

# ****************************************************************************
my $RUNMODE = ('apache' eq getpwuid($<)
              || 'daemon' eq getpwuid($<)) ? 'web' : 'command';

my ($change_startdate,$change_stopdate) = ('')x2;
if ($RUNMODE eq 'web') {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
  $change_startdate = param('start') if (param('start'));
  $change_stopdate = param('stop') if (param('stop'));
  $CHART = param('chart') || 'areaspline';
  $MODE = param('mode') || 'percent';
  $UNIT = param('unit') || 'LSMs';
  $MEASUREMENT = param('measurement') || 'discovered';
}
else {
GetOptions('start=s' => \$change_startdate,
           'stop=s'  => \$change_stopdate,
           help      => \my $HELP)
  or pod2usage(-1);
}
# Adjust parms if necessary
my($mm,$yy) = (localtime())[4,5];
my $STARTDATE = sprintf '%4d-%02d-%02d',$yy+1900,$mm+1,1;
$STARTDATE = $change_startdate if ($change_startdate);
my $today = UnixDate("today","%Y-%m-%d");
my $STOPDATE = UnixDate($today,"%Y-%m-%d");
$STOPDATE = $change_stopdate if ($change_stopdate);
$STOPDATE = $STARTDATE if ($STOPDATE lt $STARTDATE);
my $SUBTITLE = "All samples imaged $STARTDATE - $STOPDATE";
my $TERM = ($STARTDATE eq $STOPDATE)
  ? "DATE(i.create_date)='$STARTDATE'"
  : "DATE(i.create_date) BETWEEN '$STARTDATE' AND '$STOPDATE'";
if ($UNIT eq 'Lines') {
  $TERM .= ' GROUP BY 1';
}
elsif ($UNIT eq 'Samples') {
  $TERM .= ' GROUP BY 1,2,3';
}
my %sth = (
tmog => "SELECT line,ip1.value AS slidecode,ip2.value AS dataset,name,create_date FROM image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id AND ip1.type='slide_code') JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND ip2.type='data_set') WHERE $TERM ORDER BY 5",
# -----------------------------------------------------------------------------
WS_exists => "SELECT creation_date,TIMESTAMPDIFF(HOUR,?,creation_date) FROM entity WHERE entity_type='LSM Stack' AND name=?",
WS_entity => "SELECT e.id,name,ed3.value FROM entity e JOIN entityData ed1 ON (e.id=ed1.parent_entity_id AND ed1.entity_att='Line' AND ed1.value=?) JOIN entityData ed2 ON (e.id=ed2.parent_entity_id AND ed2.entity_att='Slide Code' AND ed2.value=?) LEFT OUTER JOIN entityData ed3 ON (e.id=ed3.parent_entity_id AND ed3.entity_att='Status') WHERE entity_type='Sample' AND name NOT LIKE '%-Retired' AND name NOT LIKE '%~%' ORDER BY e.id DESC",
);



# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
if (($RUNMODE eq 'web') && !(param('submit'))) {
  &displayQuery();
}
else {
  &displayCompletionStatus();
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'sage');
  &dbConnect(\$dbhw,'workstation');
  foreach (keys %sth) {
    if (/^WS/) {
      (my $n = $_) =~ s/WS_//;
      $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
  }
}


sub displayQuery
{
  &printHeader;
  my $radio = radio_group(&identify('mode'),
                          -values => ['percent','absolute'],
                          -labels => {percent => 'Percent',
                                      absolute => 'Count'},
                          -default => 'percent');
  $radio .= <<__EOT__;
<br>
<div style='width: 350px'>
<i>Percent</i> will graph each day's value as a percentage of the total number
of units tmogged that day. <i>Count</i> will graph each day's value as the
number of units for that cycle time bin.
</div>
__EOT__
  my $dd = {areaspline => 'Area spline',
            area => 'Area',
            spline => 'Spline',
            line => 'Line',
            column => 'Column'};
  my $dropdown = div({-id => 'chart_dropdown'},
                     popup_menu(&identify('chart'),
                                -values => [sort keys %$dd],
                                -labels => $dd,
                                -default => 'areaspline'));
  $dropdown .= <<__EOT__;
<div style='width: 350px'>
<i>Area spline</i> and <i>Area</i> charts work best with the <i>Percent</i>
graph mode, while <i>Column</i> works best with <i>Count</i> graph mode.
</div>
__EOT__
  my $unit = div({-id => 'unit_dropdown'},
                 popup_menu(&identify('unit'),
                            -values => [qw(LSMs Samples Lines)],
                            -default => 'LSMs'));
  $dd = {'discovered' => 'tmog -> Workstation discovery',};
#         'completed processing' => 'tmog -> Workstation completion'};
  my $meas = div({-id => 'meas_dropdown'},
                 popup_menu(&identify('measurement'),
                            -values => [sort keys %$dd],
                            -labels => $dd,
                            -default => 'tmog -> Workstation discovery'));
  print table(Tr(th('Start date'),
                 td(input({&identify('start'),
                           value => $STARTDATE}))),
              Tr(th('Stop date'),
                 td(input({&identify('stop'),
                           value => $STOPDATE}))),
              Tr(th({valign => 'top'},'Graph mode:'),
                 td($radio)),
              Tr(th({valign => 'top'},'Chart type'),
                 td($dropdown)),
              Tr(th({valign => 'top'},'Units to graph'),
                 td($unit)),
              Tr(th({valign => 'top'},'Measurement'),
                 td($meas)),
             ),br,
        div({align => 'center'},
            submit({&identify('submit'),
                    class => 'btn btn-success',
                    value => 'Submit'}));
  print end_form,&sessionFooter($Session),end_html;
}


sub displayCompletionStatus
{
  &printHeader() if ($RUNMODE eq 'web');
  my (%hash,%total);
  my @export;
  $sth{tmog}->execute();
  my $ar = $sth{tmog}->fetchall_arrayref();
  my $minimum = 2;
  my $maximum = 54;
  foreach (@$ar) {
    # Line, slide code, data set, name, date
    (my $name = $_->[3]) =~ s/.*\///;
    (my $date = $_->[4]) =~ s/ .*//;
#$date =~ s/-\d+$//;
    # ********************* Testing *********************
    my $days = $minimum + int(rand($maximum - $minimum));
    # ***************************************************
    $sth{exists}->execute($_->[4],$name);
    my($cd,$delta) = $sth{exists}->fetchrow_array();
    my $unprocessed = ($cd) ? 0 : 1;
    $total{$date}++;
    $hash{$date}{Unprocessed}++ if ($MODE eq 'percent');
    push @export,[@$_,$cd,$delta];
    if ($unprocessed) {
      $hash{$date}{Unprocessed}++ if ($MODE =~ /^abs/);
      $summary{Unprocessed}++;
    }
    else {
      $days /= 24;
      $days = $delta / 24;
      $hash{$date}{99}++ if ($MODE eq 'percent');
      if ($days > 2) {
        $hash{$date}{99}++ if ($MODE =~ /^abs/);
        $summary{99}++;
      }
      else {
        foreach my $max (1,2) {
          if ($days <= $max) {
            $hash{$date}{$max}++;
            last if ($MODE =~ /^abs/);
          }
        }
        foreach my $max (1,2) {
          if ($days <= $max) {
            $summary{$max}++;
            last;
          }
        }
      }
    }
  }
  foreach my $date (keys %hash) {
    foreach my $max (1,2,99,'Unprocessed') {
      if ($MODE eq 'percent') {
        $hash{$date}{$max} = sprintf '%0.2f',($hash{$date}{$max}/$total{$date})*100 if (exists $summary{$max});
      }
      else {
        $hash{$date}{$max} ||= 0;
      }
    }
  }
  # Build HTML
  if ($RUNMODE eq 'web') {
    my $html = '';
    # Export file
    my $export = &createExportFile(\@export,'cycle_time',
                                   ['Line','Slide code','Data set','Image',
                                    'tmog date','Completed','Delta (hours)']);
    # Table
    my @row = (Tr(th($UNIT),td(scalar(@$ar))));
    push @row,Tr(th('Unprocessed'),td($summary{Unprocessed}))
      if (exists $summary{Unprocessed});
    push @row,Tr(th('> 2 days'),td($summary{99})) if (exists $summary{99});
    push @row,Tr(th('<= 2 days'),td($summary{2})) if (exists $summary{2});
    push @row,Tr(th('<= 1 day'),td($summary{1})) if (exists $summary{1});
    my $table = div({style => 'float: left; margin: 140px 10px 0 0;'},
                    table({class => 'summary'},@row),
                    br,$export);
    # Charts
    my @colors;
    push @colors,'3333ef' if (exists $summary{Unprocessed});
    push @colors,'ed561b' if (exists $summary{99});
    push @colors,'dddf00' if (exists $summary{2});
    push @colors,'50b432' if (exists $summary{1});
    $COLORS = join(',',map {"'#$_'"} @colors);
    my $prefix = ($MODE eq 'percent') ? '% ' : '';
    my $spline = splinePercentageChart(\%hash,"$prefix$UNIT $MEASUREMENT","$STARTDATE - $STOPDATE",'spline1');
    my $bar = &barPercentageChart(\%summary,'Overall','bar1');
    # Render
    $html .= div({style => 'align: left'},
                 $table,$spline,$bar);
    $html .= div({style => 'clear: both'},'');
    print $html,end_form,&sessionFooter($Session),end_html;
  }
}


sub splinePercentageChart
{
  my($hr,$title,$subtitle,$container) = @_;
  # $hr is a reference to a hash of hashes keyed by date/cycle time description
  # $lr is a reference to a hash of lists keyed by cycle time description
  my $categories = join(',',map {"'$_'"} sort keys %$hr);
  my $lr;
  foreach my $date (sort keys %$hr) {
    foreach my $days (sort keys %{$hr->{$date}}) {
      push @{$lr->{$days}},$hr->{$date}{$days} if ($summary{$days});
    }
  }
  my $series = '';
  my @days = reverse sort keys %$lr;
  foreach my $days (@days) {
    $series .= ', ' if ($series);
    $series .= "{marker: {enabled: false},name: '";
    if ($days eq 'Unprocessed') {
      $series .= "Unprocessed', data: [";
    }
    elsif ($days == 99) {
      $series .= ">2 days', data: [";
    }
    else {
      $series .= sprintf "<= %d day%s', data: [",$days,(1 == $days) ? '' : 's';
    }
    $series .= join(',',@{$lr->{$days}}) . ']}';
  }
  my $CEILING = ($MODE =~ /^abs/) ? '' : 'ceiling: 100,';
  my $TOOLTIP = ($MODE =~ /^abs/) ? '' : "valueSuffix: '%'";
  my $sign = ($MODE eq 'percent') ? '%' : '#';
  my $code = <<__EOT__;
<div id="$container" style="position: relative; width: 800px; height: 400px; margin: 0 auto; float: left;"></div>
<script type="text/javascript">
\$(function () {
Highcharts.setOptions({
  colors: [$COLORS]
});
\$('#$container').highcharts({
  chart: {type: '$CHART'},
  credits: {enabled: false},
  title: {text: '$title',
          style: { color: 'white'},
          x: -20},
  subtitle: {text: '$subtitle',
             style: { color: '#ddd'},
             x: -20},
  xAxis: {categories: [$categories],
          labels: {style: { color: '#ddd'},rotation: -60}},
  yAxis: { title: { text: '$sign of $UNIT',
                    style: { color: 'white'} },
           labels: {style: { color: '#ddd'}},
           $CEILING
           floor: 0,
           plotLines: [{value: 0,
                        width: 1,
                        color: '#808080'
                       }]
        },
        tooltip: {$TOOLTIP},
        legend: {layout: 'vertical',
                 itemStyle: { color: '#ddd'},
                 align: 'right',
                 verticalAlign: 'middle',
                 borderWidth: 0},
  plotOptions: {area: {lineWidth: 0,
                       fillOpacity: 1},
                areaspline: {lineWidth: 0,
                             fillOpacity: 1}},
  series: [$series]
});
});
</script>
__EOT__
}


sub barPercentageChart
{
  my($hr,$title,$container) = @_;
  my $series = '';
  foreach my $days (reverse sort keys %$hr) {
    $series .= ', ' if ($series);
    $series .= sprintf "{name: '%s',data: [%d]}",$days,$hr->{$days};
  }
  my $code = <<__EOT__;
<div id="$container" style="position: relative; width: 100px; height: 300px; margin: 50px 0 0 0; float: left;"></div>
<script type="text/javascript">
\$(function () {
Highcharts.setOptions({
  colors: [$COLORS]
});
\$('#$container').highcharts({
  chart: {type: 'column'},
  credits: {enabled: false},
  legend: {enabled: false},
  title: {text: ''},
  xAxis: {categories: ['$title'],
          labels: {style: { color: '#ddd'}}},
  yAxis: {gridLineWidth: 0,
          labels: {enabled: false},
          title: {text: ''}},
  tooltip: {pointFormat: '<span style="color:{series.color}">{series.name}</span>: <b>{point.y}</b> ({point.percentage:.0f}%)<br/>',
            shared: true},
  plotOptions: {column: {stacking: 'percent'},borderColor: '#222'},
  series: [$series]
});
});
</script>
__EOT__
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d%H:%M:%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach (@$ar) {
    my @l = @$_;
    foreach my $i (1) {
      if ($l[$i] =~ /href/) {
        $l[$i] =~ s/.+=//;
        $l[$i] =~ s/".+//;
      }
    }
    $l[4] ||= ''; # Cross barcode
    print $handle join("\t",@l) . "\n";
  }
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export data");
  return($link);
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    qw(highcharts-4.0.1/highcharts
                       jquery/jquery.tablesorter tablesorter jquery/jquery-ui-latest),$PROGRAM;
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1);
  push @styles,Link({-rel=>'stylesheet',
                     -type=>'text/css',-href=>'https://code.jquery.com/ui/1.11.4/themes/ui-darkness/jquery-ui.css'});
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
