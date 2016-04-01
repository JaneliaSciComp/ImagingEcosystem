#!/usr/bin/perl

# Modes:
#   capture: capture display
#   rate: imaging rate (4-up confocal capture charts)
#   goal: performance vs. goal display
#   (none): categorized display
# Other parms:
#   all: show all families (capture display)
#   chart: chart to display (rate display, defaults to all)
#   daily: bin by day instead of month
#   family: show family display (overrides mode)
#   height: chart height (400)
#   line: display number of lines instead of number of images (rate display)
#   start: start month (rate display)
#   stop: stop month (rate display)
#   timer: rotate detailed family display (capture display)
#   width: chart width (1000)
#   ytd: show year-to-date counts (capture and rate displays)
use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use Date::Manip qw(DateCalc ParseDate UnixDate);
use DBI;
use POSIX qw(ceil);
use JFRC::Utils::Web qw(:all);
use JFRC::Highcharts qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant NBSP => '&nbsp;';
use constant USER => 'sageRead';
my $DB = 'dbi:mysql:dbname=sage;host=';
my $WSDB = 'dbi:mysql:dbname=flyportal;host=prd-db';
# Highcharts
my $pie_chart_3d = <<__EOT__;
chart: {type: 'pie',
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        options3d: {
          enabled: true,
          alpha: 45,
          beta: 0
        }
       },   
credits: {enabled: false},
__EOT__
(my $pie_chart = $pie_chart_3d) =~ s/enabled: true/enabled: false/;
my $pie_tooltip_plot = <<__EOT__;
tooltip: { pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>' },
plotOptions: { pie: { allowPointSelect: true,
                      cursor: 'pointer',
                      depth: 35,
                      dataLabels: {enabled: true,
                                   color: '#000000',
                                   connectorColor: '#000000',
                                   formatter: function() {
                                     return '<b>'+ this.point.name +'</b>: '+ Highcharts.numberFormat(this.percentage,2) +' %';
                                   }
                                  }
                    }
             },
__EOT__
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'SAGE Imagery Dashboard';
my @COLOR = qw(000066 006666 660000 666600 006600 660066);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Parameters
my $DATABASE;
my %sth = (
ANNOT => "SELECT annotated_by,family,IF(jfs_path IS NULL,'/tier2','Scality'),COUNT(1),SUM(file_size)/(1024*1024*1024*1024) FROM image_data_mv WHERE name LIKE '%lsm' AND (family LIKE ('flylight%') OR family IN ('dickson','rubin_chacrm','rubin_ssplit','split_screen_review')) GROUP BY 1,2,3",
CAPTURED => "SELECT family,DATE_FORMAT(MAX(capture_date),'%Y-%m-%d'),COUNT(2),"
            . 'COUNT(DISTINCT line),SUM(file_size) FROM '
            . 'image_data_mv GROUP BY 1',
IMAGE => "SELECT family,driver,age,imaging_project,reporter,data_set,count "
         . "FROM image_classification_mv WHERE family NOT LIKE 'fly_olympiad%' "
         . "AND family NOT LIKE '%external' AND family NOT IN "
         . "('baker_biorad','simpson_lab_grooming')",
CROSS => 'SELECT family,project_lab,cross_type,COUNT(3),COUNT(DISTINCT i.line) FROM '
         . 'image_data_mv i JOIN cross_event_vw cev ON '
         . '(i.cross_barcode=cev.cross_barcode COLLATE latin1_general_cs) '
         . 'GROUP BY 1,2,3',
RUNNING => "SELECT DATE_FORMAT(capture_date,'%Y%u'),COUNT(1) FROM "
           . 'image_vw WHERE capture_date IS NOT NULL AND family NOT LIKE '
           . "'fly_olympiad%' AND family NOT LIKE '%external' AND family "
           . "NOT IN ('baker_biorad','simpson_lab_grooming') GROUP BY 1",
LINES => 'SELECT w.week,COUNT(line) FROM (SELECT line,'
         . "MIN(DATE_FORMAT(capture_date,'%Y%u')) AS week FROM image_vw WHERE "
         . "capture_date IS NOT NULL AND family NOT LIKE 'fly_olympiad%' AND "
         . "family NOT LIKE '%external' AND family NOT IN ('baker_biorad',"
         . "'simpson_lab_grooming') GROUP BY line) AS x RIGHT OUTER JOIN "
         . "week_number w ON (x.week=w.week) WHERE w.week BETWEEN '200649' "
         . "AND DATE_FORMAT('20131018','%Y%u') GROUP BY 1",
FMONTH => "SELECT DATE_FORMAT(capture_date,'%Y%m'),c.display_name,COUNT(2),"
          . "COUNT(DISTINCT line) FROM image_vw i JOIN cv_term_vw c ON "
          . "(c.cv_term=i.family AND c.cv='family') WHERE capture_date "
          . "IS NOT NULL AND family NOT LIKE "
          . "'fly_olympiad%' AND family NOT LIKE '%external' AND family NOT IN "
          . "('baker_biorad','simpson_lab_grooming') AND "
          . "DATE_FORMAT(capture_date,'%Y%m') BETWEEN ? AND ? GROUP BY 1,2",
DMONTH => "SELECT DATE_FORMAT(capture_date,'%Y%m'),data_set,COUNT(2),"
          . "COUNT(DISTINCT line) FROM image_data_mv WHERE capture_date IS NOT "
          . "NULL AND family NOT LIKE 'fly_olympiad%' AND family NOT LIKE "
          . "'%external' AND family NOT IN ('baker_biorad',"
          . "'simpson_lab_grooming') AND DATE_FORMAT(capture_date,'%Y%m') "
          . "BETWEEN ? AND ? GROUP BY 1,2",
FSUMMARY => "SELECT DATE_FORMAT(MIN(capture_date),'%Y-%m-%d'),"
            . "DATE_FORMAT(MAX(capture_date),'%Y-%m-%d'),COUNT(1),COUNT(DISTINCT(line)),"
            . 'SUM(file_size) FROM image_data_mv WHERE family=?',
FSECDATA => 'SELECT COUNT(s.id) FROM secondary_image s JOIN image_data_mv i ON '
            . '(i.id=s.image_id) WHERE i.family=?',
FDRIVER => 'SELECT driver,COUNT(1) from image_data_mv WHERE family=? GROUP BY 1',
FPROJECT => 'SELECT imaging_project,COUNT(1) from image_data_mv WHERE family=? GROUP BY 1',
FDATA_SET => 'SELECT data_set,COUNT(1) from image_data_mv WHERE family=? GROUP BY 1',
FTILE => 'SELECT tile,COUNT(1) from image_data_mv WHERE family=? GROUP BY 1',
FDCOUNT => 'SELECT cvt.display_name,driver,imaging_project,data_set,COUNT(1) '
           . 'FROM image_data_mv i '
           . "JOIN cv_term_vw cvt ON (i.family=cvt.cv_term AND cv='family') "
           . "WHERE DATE_FORMAT(CURRENT_DATE(),'%Y%m')="
           . "DATE_FORMAT(capture_date,'%Y%m') GROUP BY 1,2,3,4",
FDCOUNTY => 'SELECT cvt.display_name,driver,imaging_project,data_set,COUNT(1) '
            . 'FROM image_data_mv i '
            . "JOIN cv_term_vw cvt ON (i.family=cvt.cv_term AND cv='family') "
            . "WHERE DATE_FORMAT(CURRENT_DATE(),'%Y')="
            . "DATE_FORMAT(capture_date,'%Y') GROUP BY 1,2,3,4",
GOAL => 'SELECT SUBSTRING(line,1,LENGTH(line)-6),COUNT(1) AS c FROM '
        . 'image_data_mv WHERE family=? AND '
        . "driver=? GROUP BY 1 HAVING SUM(IF(STRCMP(organ,'Brain'),0,1)) >= ? "
        . "AND SUM(IF(STRCMP(organ,'Ventral Nerve Cord'),0,1)) >= ?",
);
my %sthw = (
DATASET => 'SELECT ed.value,e.name,ed.owner_key FROM entityData ed '
           . 'JOIN entity e ON (e.id=ed.parent_entity_id AND '
           . "e.entity_type='Data Set') WHERE "
           . "ed.entity_att='Data Set Identifier' ORDER BY 1",
);

# **************************************************************************** # * Main                                                                     *
# ****************************************************************************

# Session authentication

my $Session = '';
unless (param('mode') eq 'capture' || param('mode') eq 'rate') {
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
}

# Connect to database
$DATABASE = lc(param('_database') || 'prod');
$DB .= ($DATABASE eq 'prod') ? 'mysql3' : 'db-dev';
my $dbh = DBI->connect($DB,(USER)x2,{RaiseError=>1,PrintError=>0});
if (param('daily')) {
  $sth{DMONTH} =~ s/%Y%m/%Y%m%d/g;
  $sth{FMONTH} =~ s/%Y%m/%Y%m%d/g;
}
$sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  foreach (keys %sth);
my $dbhw = DBI->connect($WSDB,('flyportalRead')x2,{RaiseError=>1,PrintError=>0});
$sthw{$_} = $dbhw->prepare($sthw{$_}) || &terminateProgram($dbhw->errstr)
  foreach (keys %sthw);

# Main processing
if (param('family')) {
  &showFamilyDashboard();
}
elsif (param('mode')) {
  if (param('mode') eq 'rate') {
    &showRateDashboard();
  }
  elsif (param('mode') eq 'capture') {
    &showCaptureDashboard();
  }
  elsif (param('mode') eq 'goal') {
    &showGoalDashboard();
  }
  elsif (param('mode') eq 'annotator') {
    &showAnnotDashboard();
  }
}
else {
  &showStandardDashboard();
}
# ----- Footer -----
print div({style => 'clear: both;'},NBSP),end_form,
      &sessionFooter($Session),end_html;

# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

# ****************************************************************************
# * Subroutine:  showStandardDashboard                                       *
# * Description: This routine will show the standard dashboard.              *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showStandardDashboard
{
  # ----- Page header -----
  print &pageHead(),start_form,&hiddenParameters();
  my %panel;
  my $ar;

  # Imagery on workstation
  $sthw{DATASET}->execute();
  $ar = $sthw{DATASET}->fetchall_arrayref();
  my %ws_map;
  foreach (@$ar) {
    $ws_map{$_->[0]}{name} = $_->[1];
    $ws_map{$_->[0]}{owner} = $_->[2];
  }

  # Imagery by family
  $sth{CAPTURED}->execute();
  $ar = $sth{CAPTURED}->fetchall_arrayref();
  my ($td,$ti);
  foreach (@$ar) {
  	$_->[0] = a({href => '?family='.$_->[0],
  		         target => '_blank'},$_->[0]);
    $ti += $_->[2];
    $_->[-1] ||= 0;
    $td += $_->[-1];
    $_->[-1] = sprintf '%.2f',$_->[-1]/(1024**4);
  }
  $panel{captured} =
        h3('Imagery by family')
        . table({class => 'sortable',&identify('standard')},
                Tr(th(['Family','Last capture','Count','Line count','Size (TB)'])),
                (map { Tr(td($_)) } @$ar),
                tfoot(Tr(td(['TOTAL','',$ti,'',sprintf '%.2f',$td/(1024**4)])))
               );
  $panel{captured_pie} = &familyPieChart($ar);

  # Imagery by cross
#  $sth{CROSS}->execute();
#  $ar = $sth{CROSS}->fetchall_arrayref();
#  $panel{crosses} =
#        h3('Imagery by cross')
#        . table({class => 'sortable',&identify('standard')},
#                Tr(th(['Family','Lab','Cross type','Count','Line count'])),
#                map { Tr(td($_)) } @$ar);

  # Imagery by dataset
  $sth{IMAGE}->execute();
  $ar = $sth{IMAGE}->fetchall_arrayref();
  foreach (@$ar) {
    splice @$_,-1,0,'';
    if ($_->[5] && exists $ws_map{$_->[5]}) {
      ($_->[6] = $ws_map{$_->[5]}{owner}) =~ s/.+://;
      $_->[5] = span({style => 'color: #009900;'},$ws_map{$_->[5]}{name});
    }
  }
  $panel{dataset} =
        h3('Categorized confocal imagery')
        . 'Data sets in ' . span({style => 'color: green;'},'green')
        . ' are in the Janelia Workstation' . br
        . table({class => 'sortable',&identify('standard')},
                Tr(th(['Family','Driver','Age','Imaging project','Reporter','Data set','Owner','Count'])),
                map { Tr(td($_)) } @$ar);

  # Render
  print div({style => 'float: left; padding-right: 15px;'},
            $panel{captured},br,$panel{crosses}),br,
        div({style => 'float: left; padding-right: 15px;'},
            $panel{captured_pie}),br,
        div({style => 'float: left; padding-right: 15px;'},
            $panel{dataset});
}


sub familyPieChart
{
  my $ar = shift;
  my @element;
  my $data;
  my @sorted = sort { $b->[2] <=> $a->[2] } @$ar;
  my $acc;
  foreach (@sorted) {
    next if ($_->[0] =~ /(?:fly_olympiad|external|biorad|grooming)/);
    if (scalar(@element) <= 8) {
      push @element,"['$_->[0]',$_->[2]]";
    }
    else {
      $acc += $_->[2];
    }
  }
  push @element,"['Other',$acc]";
  $data = "data: [\n" . join(",\n",@element) . "\n]";
  my $width = (param('width') || '600') . 'px';
  $a = <<__EOT__;
<div id="container" style="width: $width; height: $width; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
  Highcharts.getOptions().colors = Highcharts.map(Highcharts.getOptions().colors, function(color) {
		      return {
		          radialGradient: { cx: 0.5, cy: 0.3, r: 0.7 },
		          stops: [
		              [0, color],
		              [1, Highcharts.Color(color).brighten(-0.3).get('rgb')] // darken
		          ]
		      };
		  });
        \$('#container').highcharts({
            $pie_chart_3d
            title: { text: 'Confocal imagery by family' },
            $pie_tooltip_plot
            series: [{
                type: 'pie',
                name: 'Confocal imagery',
                $data
            }]
        });
    });
</script>
__EOT__
  return($a);
}


# ****************************************************************************
# * Subroutine:  showCaptureDashboard                                        *
# * Description: This routine will show the rate dashboard.                  *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showCaptureDashboard
{
  my $ALL = param('all');
  push @BREADCRUMBS,($APPLICATION,'?','Capture dashboard');
  print &pageHead(),start_form,&hiddenParameters();
  my $statement = (param('ytd')) ? 'FDCOUNTY' : 'FDCOUNT';
  $sth{$statement}->execute();
  my $ar = $sth{$statement}->fetchall_arrayref();
  my (%family,%info);
  foreach (@$ar) {
    next if ($_->[0] =~ /^Fly Olympiad/ && !$ALL);
    $family{$_->[0]}{COUNT} += $_->[-1];
    $info{$_->[0]} .= Tr(td([$_->[1],$_->[2],$_->[3],$_->[4]]));
  }
  my $content = '';
  my $i = 0;
  my $period = (param('ytd')) ? 'year' : 'month';
  unless (scalar keys %family) {
    print div({class => 'centered'},
              h1("No images have been captured yet for this $period"));
    return;
  }
  my $num = 1;
  foreach (sort keys %family) {
    (my $pid = $_) =~ s/ +/_/g;
    my %option = ();
    unless (/^Fly Olympiad/) {
      $pid = 'P_' . $num++;
      %option = (onClick => "toggleVis('$pid')");
    }
    my $class = (param('ytd')) ? 'countboxy' : 'countbox';
    $content .= div({class => 'familybox',
                     %option,
                     style => 'background-color: #'.$COLOR[$i]},$_)
                . div({class => $class,
                       %option,
                       style => 'border: 2px solid #'.$COLOR[$i],
                      },$family{$_}{COUNT});
    if (exists $info{$_}) {
      $info{$_} = Tr(th(['Driver','Project','Data set','Count'])) . $info{$_};
      $content .= div({id => $pid,
                       style => 'display: none;border: 2px solid #'.$COLOR[$i],
                       class => 'info'},
                      h3('Imagery details'),table({&identify('standard')},$info{$_}));
    }
    $i++;
    $i = 0 if ($i > 5);
  }
  print div({class => 'centered'},
            h1((($ALL) ? 'Imagery' : 'LSM'),"files captured this $period"),br,
            div({style => 'float: left;'},$content));
  if (param('timer')) {
    my $maxpid = $num - 1;
  print <<__EOT__;
<script>
whichPID = 0;
maxPID = $maxpid;
window.setTimeout(switchDetails,8000);
function switchDetails() {
  if (whichPID > 0) {
    toggleVis('P_'+whichPID);
  }
  whichPID++;
  if (whichPID > maxPID) {
    whichPID = 1;
  }
  toggleVis('P_'+whichPID);
  window.setTimeout(switchDetails,8000);
}
</script>
__EOT__
  }
}


# ****************************************************************************
# * Subroutine:  showAnnotDashboard                                          *
# * Description: This routine will show the annotator dashboard.             *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showAnnotDashboard
{
  push @BREADCRUMBS,($APPLICATION,'?','Annotator dashboard');
  print &pageHead(),start_form,&hiddenParameters();
  $sth{ANNOT}->execute();
  my $ar = $sth{ANNOT}->fetchall_arrayref();
  my (%annot,%count,%person,%tier,%total);
  foreach (@$ar) {
    $_->[0] ||= '(unknown)';
    $total{count} += $_->[-2];
    $total{size} += $_->[-1];
    $annot{(split(' ',$_->[0]))[-1]}{$_->[2]} += $_->[-1];
    $person{(split(' ',$_->[0]))[-1]} += $_->[-1];
    $count{$_->[2]} += $_->[-2];
    $tier{$_->[2]} += $_->[-1];
    $_->[-1] = sprintf '%.2f',$_->[-1];
  }
  foreach my $a (sort keys %annot) {
    if ($person{$a} < 2) {
      delete $annot{$a};
      next;
    }
    foreach my $l (sort keys %{$annot{$a}}) {
      $annot{$a}{$l} = sprintf '%.2f',$annot{$a}{$l};
    }
  }
  $total{size} = sprintf '%.2f',$total{size};
  $tier{$_} = sprintf '%.2f',$tier{$_} foreach (keys %tier);
  my @color = ('#33ff33','#ff3333','#3333ff','#33cc33','#cc3333','#3333cc',
               '#339933','#993333','#333399','#336633','#663333','#333366',
               '#33cccc','#cc33cc','#cccc33','#339999','#993399','#999933',
               '#336666','#663366','#666633');
  my $chart = &generateSubdividedPieChart(hashref => \%annot,
                                          title => 'LSMs by annotator (TB)',
                                          subtitle => 'Subdivided by location',
                                          content => 'graph1',
                                          color => \@color,
                                          unit => 'TB',
                                          point_format => '<b>{point.y}TB</b>: '
                                                    .'{point.percentage:.1f}%');
  print $chart;
  my $chart2 = &generateSimplePieChart(hashref => \%count,
                                       title => 'LSMs by location (count)',
                                       content => 'graph2',
                                       width => '400px', height => '400px',
                                       color => ['#cc3333','#33cc33'],
                                       point_format => '<b>{point.y}</b>: '
                                                    .'{point.percentage:.1f}%');
  my $chart3 = &generateSimplePieChart(hashref => \%tier,
                                       title => 'LSMs by location (TB)',
                                       content => 'graph3',
                                       width => '400px', height => '400px',
                                       color => ['#cc3333','#33cc33'],
                                       unit => 'TB',
                                       point_format => '<b>{point.y}TB</b>: '
                                                    .'{point.percentage:.1f}%');
  my $lower = div({style => 'float: left;'},
                  table({class => 'sortable',&identify('standard')},
                        thead(Tr(td(['Annotator','Family','Location','Count',
                                     'Size (TB)']))),
                        tbody(map {Tr(td($_))} @$ar),
                        tfoot(Tr(th(['TOTAL','','',$total{count},
                                     $total{size}])))))
              . div({style => 'float: left;'},$chart2,br,$chart3);
  print $lower;
}


# ****************************************************************************
# * Subroutine:  showGoalDashboard                                           *
# * Description: This routine will show the goal dashboard.                  *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showGoalDashboard
{
  push @BREADCRUMBS,($APPLICATION,'?','Goal dashboard');
  print &pageHead(),start_form,&hiddenParameters();
  my @G = (['Dickson GAL4',2800,'dickson','GAL4_Collection',1,1],
           ['Dickson LexA',1200,'dickson','LexA',1,1],
           ['Rubin LexA',608,'rubin_chacrm','LexA',1,1]);
  my @info;
  my $content = '';
  my $i = 0;
  foreach (@G) {
    my @arr = @$_;
    my $title = shift @arr;
    my $line_goal = shift @arr;
    my $ar = &getProgressToGoal(@arr);
    $content .= div({class => 'familybox',
                     style => 'background-color: #'.$COLOR[$i]},
                    $title)
                . div({class => 'countbox',
                       style => 'border: 2px solid #'.$COLOR[$i]},
                      (sprintf '%.2f%%',scalar(@$ar)/$line_goal*100));
    $i++;
  }
  print div({class => 'centered'},
            h1('Progress toward goal'),br,
            div({style => 'float: left;'},$content));
}


sub getProgressToGoal
{
  $sth{GOAL}->execute(@_);
  my $ar = $sth{GOAL}->fetchall_arrayref();
  return($ar);
}


# ****************************************************************************
# * Subroutine:  showRateDashboard                                           *
# * Description: This routine will show the rate dashboard.                  *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showRateDashboard
{
  # ----- Page header -----
  push @BREADCRUMBS,($APPLICATION,'?','Rate dashboard');
  print &pageHead(),start_form,&hiddenParameters();
  my %panel;
  my $ar;
  my $line = param('line') || 0;
  my $term = ($line) ? 'lines' : 'images';
  my $chart = param('chart') || 'all';
  if ($chart eq 'all' || $chart ne 'family' || $chart ne 'dataset') {
    # Images/line per week
    my $statement = ($line) ? 'LINES' : 'RUNNING';
    $sth{$statement}->execute();
    $ar = $sth{$statement}->fetchall_arrayref();
    $panel{weeklyrate} = &zoomChart($ar,"Confocal $term captured per week",$term,'weeklyrate');
    foreach (1..$#$ar) {
      $ar->[$_][-1] += $ar->[$_-1][-1];
    }
    $panel{running} = &zoomChart($ar,"Confocal $term running total",$term,'running',2);
  }
  # Images/lines per month (by family)
  my $stop = param('stop') || UnixDate("today","%Y%m");
  my $start = param('start') || UnixDate(DateCalc($stop,'- 1 year'),"%Y%m");
  if (param('daily')) {
    $stop = UnixDate("today","%Y%m%d");
    $start = UnixDate("today","%Y%m") . '01';
  }
  ($start = $stop) =~ s/..$/01/ if (param('ytd'));
  my $column = ($line) ? 2 : 3;
  if ($chart eq 'all' || $chart eq 'family') {
    $sth{FMONTH}->execute($start,$stop);
    $ar = $sth{FMONTH}->fetchall_arrayref();
    splice(@$_,$column,1) foreach (@$ar);
    $panel{family} = &lineChart($ar,"Confocal $term captured per month by family",
                                $term,$start,$stop,'family');
  }
  # Images/lines per month (by data set)
  if ($chart eq 'all' || $chart eq 'dataset') {
    $sth{DMONTH}->execute($start,$stop);
    $ar = $sth{DMONTH}->fetchall_arrayref();
    foreach (@$ar) {
      splice(@$_,$column,1);
      $_->[1] ||= '(no data set)';
    }
    $panel{dataset} = &lineChart($ar,"Confocal $term captured per month by data set",
                                 $term,$start,$stop,'dataset');
  }
  # Render
  my @display = ($chart eq 'all') ? qw(running weeklyrate family dataset)
                                  : ($chart);
  print map {div({&identify("chart_$_"),style => 'float: left;'},$panel{$_})} @display;
}


sub zoomChart
{
  my($ar,$title,$y_axis,$container,$color) = @_;
  $color ||= 0;
  my $data = '['
             . join(',',map {$_->[1]} @$ar)
             . ']';
  my $width = (param('width') || '1000') . 'px';
  my $height = (param('height') || '400') . 'px';
  $a = <<__EOT__;
<div id="$container" style="width: $width; height: $height; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
        Highcharts.setOptions({
            lang: {numericSymbols: []}
        });
        \$('#$container').highcharts({
            chart: {
                zoomType: 'x',
                spacingRight: 20
            },
            credits: {enabled: false},
            title: { text: '$title' },
            subtitle: {
                text: document.ontouchstart === undefined ?
                    'Click and drag in the plot area to zoom in' :
                    'Pinch the chart to zoom in'
            },
            xAxis: {
                type: 'datetime',
                maxZoom: 7 * 24 * 3600000, // 1 week
                title: { text: null }
            },
            yAxis: {
                min: 0,
                title: { text: '# $y_axis' }
            },
            tooltip: { shared: true },
            legend: { enabled: false },
            plotOptions: {
                area: {
                    fillColor: {
                        linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1},
                        stops: [
                            [0, Highcharts.getOptions().colors[$color]],
                            [1, Highcharts.Color(Highcharts.getOptions().colors[$color]).setOpacity(0).get('rgba')]
                        ]
                    },
                    lineColor: Highcharts.getOptions().colors[$color],
                    lineWidth: 1,
                    marker: { enabled: false },
                    shadow: false,
                    states: { hover: { lineWidth: 1 } },
                    threshold: null
                }
            },
    
            series: [{
                type: 'area',
                name: 'Images',
                pointInterval: 7 * 24 * 3600000, // 1 week
                pointStart: Date.UTC(2006,11,04),
                data: $data
            }]
        });
    });
</script>
__EOT__
  return($a);
}


sub lineChart
{
  my($ar,$title,$y_axis,$start,$stop,$container) = @_;
  my $subtitle = "($start-$stop)";
  my (%date,%date_list,%family,%series);
  foreach (@$ar) {
    $date_list{$_->[0]}++;
    $date{$_->[0]}{$_->[1]} = $_->[2];
    $family{$_->[1]}++;
  }
  my @series;
  foreach my $family (sort keys %family) {
    my @arr = ();
    foreach my $date (sort keys %date_list) {
      push @arr,$date{$date}{$family} || 0;
    }
    push @series,"{name: '$family',\ndata: ["
                 . join(',',@arr) . ']}';
  }
  my $series = 'series: [' . join(",\n",@series) . ']';
  my $categories = join(',',sort keys %date_list);
  my $width = (param('width') || '1000') . 'px';
  my $height = (param('height') || '400') . 'px';
  $a = <<__EOT__;
<div id="$container" style="width: $width; height: $height; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
        \$('#$container').highcharts({
            credits: {enabled: false},
            title: {
                text: '$title',
                x: -20 //center
            },
            subtitle: {
                text: '$subtitle',
                x: -20 //center
            },
            xAxis: { categories: [$categories] },
            yAxis: {
                min: 0,
                title: { text: '# $y_axis' },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            legend: {
                layout: 'vertical',
                align: 'right',
                verticalAlign: 'middle',
                borderWidth: 0
            },$series
        });
    });
</script>
__EOT__
  return($a);
}


# ****************************************************************************
# * Subroutine:  showFamilyDashboard                                         *
# * Description: This routine will show the family dashboard.                *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     NONE                                                        *
# ****************************************************************************
sub showFamilyDashboard
{
  # ----- Page header -----
  push @BREADCRUMBS,($APPLICATION,'?','Family dashboard');
  print &pageHead(),start_form,&hiddenParameters();
  my %panel;
  my $family = param('family');
  # Summary
  $sth{FSUMMARY}->execute($family);
  my $ar = $sth{FSUMMARY}->fetchrow_arrayref();
  $sth{FSECDATA}->execute($family);
  my($secdata) = $sth{FSECDATA}->fetchrow_array();
  my @row = (Tr(td(['Number of images:',&commify($ar->[2])])),
             Tr(td(['Distinct lines:',&commify($ar->[3])])),
             Tr(td(['Confocal image space:',(sprintf '%.2fTB',$ar->[4]/(1024**4))])),
             Tr(td(['Secondary images:',&commify($secdata)])));
  push @row,Tr(td(['Capture range:',$ar->[0].' - '.$ar->[1]]))
    if ($ar->[0] && $ar->[1]);
  print div({class => 'boxed'},
            div({align => 'center'},h2($family)),
            table(@row)
           ),br;
  # Pie charts
  foreach my $term (qw(driver project data_set tile)) {
    $sth{'F'.uc($term)}->execute($family);
    $ar = $sth{'F'.uc($term)}->fetchall_arrayref();
    my %hash = map { $_->[0] => $_->[1]*1 } @$ar;
    if (exists $hash{''}) {
      $hash{'(none)'} = $hash{''};
      delete $hash{''};
    }
    $panel{$term} = &generateSimplePieChart(hashref => \%hash,
                                            title => ucfirst($term),
                                            content => $term,
                                            legend => 'right',
                                            sort => 'value',
                                            width => 600, height => 400);
  }
  
  foreach (qw(driver project data_set tile)) {
    print div({style => 'float: left;'},
              $panel{$_});
  }
}


sub commify
{
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}


# ****************************************************************************
# * Subroutine:  hiddenParameters                                            *
# * Description: This routine will return HTML for hidden parameters.        *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     HTML                                                        *
# ****************************************************************************
sub hiddenParameters
{
  hidden(&identify('_database'),default=>$DATABASE);
}


# ****************************************************************************
# * Subroutine:  pageHead                                                    *
# * Description: This routine will return the page header.                   *
# *                                                                          *
# * Parameters:  Named parameters                                            *
# *              title: page title                                           *
# *              mode:  mode (initial, list, or feather)                     *
# * Returns:     HTML                                                        *
# ****************************************************************************
sub pageHead
{
  my %arg = (title => $APPLICATION,
             mode  => 'initial',
             @_);
  my @scripts = ();
  if ($arg{mode} eq 'initial') {
    push @scripts,map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                      qw(jquery/jquery-latest highcharts-4.0.1/highcharts
                         highcharts-4.0.1/highcharts-3d
                         highcharts-4.0.1/modules/exporting sorttable),$PROGRAM;
  }
  $arg{title} .= ' (Development)' if ($DATABASE eq 'dev');
  my @styles;
  push @styles,map { Link({-rel=>'stylesheet',
                           -type=>'text/css','-href',@$_}) }
                         (['http://fonts.googleapis.com/css?family=Quicksand:400,700']);
  &standardHeader(title       => $arg{title},
                  css_prefix  => $PROGRAM,
                  script      => \@scripts,
                  style       => \@styles,
                  breadcrumbs => \@BREADCRUMBS,
                  expires     => 'now');
}
