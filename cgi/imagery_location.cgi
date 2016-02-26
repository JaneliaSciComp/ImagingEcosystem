#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use Data::Dumper;
use DBI;
use POSIX qw(ceil);
use JFRC::Utils::Web qw(:all);

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
our $APPLICATION = 'Imagery Location Dashboard';
my %COLOR = (Scality => '#55ee55',
             dm11 => '#ee5555',
             tier2 => '#eeee55',
             'tier2 uncompressed' => '#ff9933',
);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Parameters
my $DATABASE;
my %sth = (
IMAGE => "SELECT family,path,jfs_path,file_size,DATE(create_date) FROM "
         . "image_data_mv WHERE family NOT LIKE 'simpson%' "
         . "AND family NOT LIKE 'rubin%' AND family NOT LIKE 'truman%' "
         . "AND family NOT IN ('baker_lab','flylight_rd','heberlein_central_brain','leet_chacrm',"
         . "'leet_discovery','rubin_lab_2','rubin_rd_split',"
         . "'zlatic_peripheral') AND name LIKE '%lsm'",
IMAGEA => "SELECT family,path,jfs_path,file_size,DATE(create_date) FROM image_data_mv WHERE "
          . "name LIKE '%lsm' AND (path IS NOT NULL OR jfs_path IS NOT NULL)",
);

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication

our $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));

# Connect to database
$DATABASE = lc(param('_database') || 'prod');
$DB .= ($DATABASE eq 'prod') ? 'mysql3' : 'db-dev';
my $dbh = DBI->connect($DB,(USER)x2,{RaiseError=>1,PrintError=>0});
$sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  foreach (keys %sth);

# Main processing
&showStandardDashboard();
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

  # Imagery location
  my %family;
  my $cursor = (param('all')) ? 'IMAGEA' : 'IMAGE';
  $sth{$cursor}->execute();
  my $ar = $sth{$cursor}->fetchall_arrayref();
  my (%count,%size);
  foreach (@$ar) {
    # family,path,jfs_path,file_size,create_date
    my($family,$path,$jfs_path,$file_size,$create_date) = @$_;
    $file_size ||= 0;
    $family{$family}{nofs}++ unless ($file_size);
    if (!$path && !$jfs_path) {
      $family{$family}{error}++;
    }
    elsif (!$path && $jfs_path) {
      $family{$family}{jfs}++;
      $family{$family}{jfsfs} += $file_size;
      $count{Scality}++;
      $size{Scality} += $file_size;
    }
    elsif ($path && $jfs_path) {
      $family{$family}{dual}++;
    }
    else {
      my $disk = 'tier2';
      $disk = 'dm11' if ($path =~ /\/groups/);
      $count{($disk eq 'tier2' && $path =~ /lsm$/) ? 'tier2 uncompressed' : $disk}++;
      $size{($disk eq 'tier2' && $path =~ /lsm$/) ? 'tier2 uncompressed' : $disk} += $file_size;
      $family{$family}{($path =~ /lsm$/) ? $disk.'u' : $disk.'c'}++;
      $family{$family}{($path =~ /lsm$/) ? $disk.'ufs' : $disk.'cfs'} += $file_size;
      if ($path =~ /lsm$/) {
        $family{$family}{oldest} = $create_date
          if (!$family{$family}{oldest} || $create_date lt $family{$family}{oldest});
        $family{$family}{newest} = $create_date
          if (!$family{$family}{newest} || $create_date gt $family{$family}{newest});
      }
    }
  }

  my %sum;
  my @row;
  my $dual = 0;
  $dual += $family{$_}{dual} foreach (keys %family);
  my @acc = qw(jfs jfsfs tier2c tier2cfs tier2u tier2ufs dm11u dm11ufs);
  foreach (sort keys %family) {
    foreach my $t (@acc) {
      $sum{$t} += $family{$_}{$t};
    }
    my @col = (td($_),&renderColumns($family{$_}{jfs},$family{$_}{jfsfs},$COLOR{Scality}),
               &renderColumns($family{$_}{tier2c},$family{$_}{tier2cfs},$COLOR{tier2}),
               &renderColumns($family{$_}{tier2u},$family{$_}{tier2ufs},$COLOR{'tier2 uncompressed'}),
               &renderColumns($family{$_}{dm11u},$family{$_}{dm11ufs},$COLOR{dm11}));
    push @col,td($family{$_}{dual}) if ($dual);
    push @col,td($family{$_}{oldest});
    push @row,[@col];
  }
  my @header = ('Family','Scality','JFS size (TB)',
                'tier2 compressed','tier2 compressed size (TB)',
                'tier2 uncompressed','tier2 uncompressed size (TB)',
                'dm11 uncompressed','dm11 uncompressed size (TB)',
                'Dual locations','Oldest uncompressed');
  splice @header,9,1 unless ($dual);
  my @col = (th('TOTAL'),
             &renderColumns($sum{jfs},$sum{jfsfs},$COLOR{Scality},1),
             &renderColumns($sum{tier2c},$sum{tier2cfs},$COLOR{tier2},1),
             &renderColumns($sum{tier2u},$sum{tier2ufs},$COLOR{'tier2 uncompressed'},1),
             &renderColumns($sum{dm11u},$sum{dm11ufs},$COLOR{dm11},1),
             td(['','']));
  $panel{image} =
        h3('LSM location by family'
           . ((param('all')) ? '' : ' for Workstation-managed imagery'))
        . table({class => 'sortable',&identify('standard')},
                thead(Tr(th([@header]))),
                tbody(map {Tr(@$_)} @row),
                tfoot(Tr(@col))
               );
  @$ar = ();
  push @$ar,[$_,$count{$_}] foreach (sort keys %count);
  $panel{countpie} = &pieChart($ar,'# Images per storage tier','countpie');
  @$ar = ();
  push @$ar,[$_,$size{$_}/(1024**4)] foreach (sort keys %size);
  $panel{sizepie} = &pieChart($ar,'Storage footprint per storage tier','sizepie');
  # Render
  print div({style => 'float: left; padding-right: 15px;'},
            $panel{image},br,
            div({style => 'float: left'},$panel{countpie}),
            div({style => 'float: left'},$panel{sizepie}));
}


sub renderColumns
{
  my($count,$size,$color,$bold) = @_;
  return(td(['',''])) unless ($count);
  $bold = ($bold) ? '; font-weight: bold;' : '';
  td({style => 'background: '.$color.$bold},[&commify($count),
                                             sprintf '%.2f',($size)/(1024**4)]);
}


sub renderSize
{
  my $size = shift;
  return('') unless ($size);
  sprintf '%.2f',($size)/(1024**4);
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


sub pieChart
{
  my($ar,$title,$container) = @_;
  my $data;
  my @element = map { "['" . ($_->[0]||'(none)') . "'," . $_->[1] . ']'} @$ar;
  $data = "data: [\n" . join(",\n",@element) . "\n]";
  my @color;
  push @color,"'".$COLOR{$_->[0]}."'" foreach (@$ar);
  my $color = join(',',@color);
  $a = <<__EOT__;
<div id="$container" style="width: 600px; height: 400px; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
        Highcharts.setOptions({
          colors: [$color]
        });
        \$('#$container').highcharts({
            $pie_chart
            title: { text: '$title' },
            $pie_tooltip_plot
            series: [{
                type: 'pie',
                name: '$title',
                $data
            }]
        });
    });
</script>
__EOT__
  return($a);
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
