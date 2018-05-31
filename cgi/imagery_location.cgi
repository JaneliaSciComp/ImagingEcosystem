#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use Data::Dumper;
use Date::Calc qw(Delta_Days);
use DBI;
use POSIX qw(ceil);
use JFRC::Highcharts qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant NBSP => '&nbsp;';
use constant USER => 'sageRead';
my $DB = 'dbi:mysql:dbname=sage;host=';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Imagery Location Dashboard';
my %COLOR = (Scality => '#55ee55',
             dm11 => '#eeee55',
             other=> '#ee5555',
             'dm11 uncompressed' => '#ff9933',
             'other uncompressed' => '#ee5555',
);
my @COLOR = ('#55ee55','#eeee55','#ff9933','#ee5555','#ee5555');

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Parameters
my $DATABASE;
my %sth = (
ALL_FAMILY => "SELECT path,file_size,DATE(create_date) FROM "
              . "image_data_mv WHERE family=? AND path IS NOT NULL AND "
              . "path NOT LIKE '%lsm'",
FAMILY => "SELECT path,file_size,DATE(create_date) FROM "
          . "image_data_mv WHERE family=? AND path IS NOT NULL AND "
          . "path LIKE '%lsm'",
FL_LSMS => "SELECT family,path,jfs_path,file_size,DATE(create_date) FROM "
           . "image_data_mv WHERE family NOT LIKE 'simpson%' "
           . "AND family NOT LIKE 'rubin_wu%' AND family NOT LIKE 'truman%' "
           . "AND family NOT IN ('baker_lab','flylight_rd','heberlein_central_brain','leet_chacrm',"
         . "'leet_discovery','rubin_lab_2','rubin_rd_split',"
         . "'zlatic_peripheral') AND name LIKE '%lsm'",
ALL_LSMS => "SELECT family,path,jfs_path,file_size,DATE(create_date) FROM "
            . "image_data_mv WHERE name LIKE '%lsm' AND (path IS NOT NULL "
            . "OR jfs_path IS NOT NULL)",
ALL_OTHER => "SELECT family,path,jfs_path,file_size,DATE(create_date) FROM "
             . "image_data_mv WHERE name NOT LIKE '%lsm' AND (path IS NOT NULL "
             . "OR jfs_path IS NOT NULL)",
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
if (param('family')) {
  &showFamilyDetails();
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

sub showFamilyDetails
{
  # ----- Page header -----
  print &pageHead(),start_form,&hiddenParameters();
  my $cursor = (param('other')) ? 'ALL_FAMILY' : 'FAMILY';
  $sth{$cursor}->execute(my $family = param('family'));
  my $ar = $sth{$cursor}->fetchall_arrayref();
  print "$family uncompressed LSMs: ",scalar(@$ar),br;
  my (%bin,%sbin);
  my @today = qw(2016 03 29);
  foreach (@$ar) {
    my $days = Delta_Days(split(/[-T]/,$_->[-1]),@today);
    if ($days <= 30) {
      $bin{'<= 30 days'}++;
      $sbin{'<= 30 days'} += $_->[1];
    }
    elsif ($days <= 60) {
      $bin{'30-60 days'}++;
      $sbin{'30-60 days'} += $_->[1];
    }
    elsif ($days <= 90) {
      $bin{'60-90 days'}++;
      $sbin{'60-90 days'} += $_->[1];
    }
    elsif ($days <= 180) {
      $bin{'3-6 months'}++;
      $sbin{'3-6 months'} += $_->[1];
    }
    else {
      $bin{'> 6 months'}++;
      $sbin{'> 6 months'} += $_->[1];
    }
  }
  my (@bin,@sbin);
  foreach ('<= 30 days','30-60 days','60-90 days','3-6 months','> 6 months') {
    push @bin,[$_,$bin{$_}] if (exists $bin{$_});
    push @sbin,[$_,1*sprintf "%.2f",$sbin{$_}/(1024**4)] if (exists $sbin{$_});
  }
my $histogram = &generateHistogram(arrayref => \@bin,
                                   title => 'LSM age (# files)',
                                   content => 'age',
                                   yaxis_title => '# files',
                                   color => '#66f',
                                   width => '600px', height => '500px');
  my $shistogram = &generateHistogram(arrayref => \@sbin,
                                      title => 'LSM age (space in TB)',
                                      content => 'space',
                                      yaxis_title => 'Total space (TB)',
                                      color => '#6f6',
                                      width => '600px', height => '500px');
  print $histogram,br,$shistogram;
}


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
  my $cursor = (param('all')) ? 'ALL_LSMS' : 'FL_LSMS';
  $cursor = 'ALL_OTHER' if (param('other'));
  $sth{$cursor}->execute();
  my $ar = $sth{$cursor}->fetchall_arrayref();
  my (%count,%size);
  foreach (@$ar) {
    # family,path,jfs_path,file_size,create_date
    my($family,$path,$jfs_path,$file_size,$create_date) = @$_;
    $file_size ||= 0;
    $family{$family}{nofs}++ unless ($file_size);
    if (!$path && !$jfs_path) {
      # Error
      $family{$family}{error}++;
    }
    elsif (!$path && $jfs_path) {
      # Scality
      $family{$family}{jfs}++;
      $family{$family}{jfsfs} += $file_size;
      $count{Scality}++;
      $size{Scality} += $file_size;
    }
    elsif ($path && $jfs_path) {
      # Dual
      $family{$family}{dual}++;
    }
    else {
      my $disk = 'other';
      $disk = 'dm11' if ($path =~ /\/groups/);
      if ($disk eq 'dm11') {
        $count{($path !~ /bz2$/) ? 'dm11 uncompressed' : $disk}++;
        $size{($path !~ /bz2$/) ? 'dm11 uncompressed' : $disk} += $file_size;
      }
      else {
        $count{($path !~ /bz2$/) ? 'other uncompressed' : $disk}++;
        $size{($path !~ /bz2$/) ? 'other uncompressed' : $disk} += $file_size;
      }
      $family{$family}{($path !~ /bz2$/) ? $disk.'u' : $disk.'c'}++;
      $family{$family}{($path !~ /bz2$/) ? $disk.'ufs' : $disk.'cfs'} += $file_size;
      if ($path !~ /bz2$/) {
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
  $dual += ($family{$_}{dual}||0) foreach (keys %family);
  my @acc = qw(jfs jfsfs otheru otherufs dm11c dm11cfs dm11u dm11ufs);
  foreach (sort keys %family) {
    foreach my $t (@acc) {
      $sum{$t} += ($family{$_}{$t}||0);
    }
    my @col = (td($_),&renderColumns($family{$_}{jfs},$family{$_}{jfsfs},$COLOR{Scality}),
               &renderColumns($family{$_}{dm11c},$family{$_}{dm11cfs},$COLOR{dm11}),
               &renderColumns($family{$_}{dm11u},$family{$_}{dm11ufs},$COLOR{'dm11 uncompressed'}),
               &renderColumns($family{$_}{otheru},$family{$_}{otherufs},$COLOR{'other uncompressed'}),
              );
    push @col,td($family{$_}{dual}) if ($dual);
    push @col,td(a({href => "?family=$_"
                    . ((param('other')) ? ';other=1' : ''),
                    target => '_blank'},$family{$_}{oldest}));
    push @row,[@col];
  }
  my @header = ('Family','Scality','JFS size (TB)',
                'dm11 compressed','dm11 compressed size (TB)',
                'dm11 uncompressed','dm11 uncompressed size (TB)',
                'other uncompressed','other uncompressed size (TB)',
                'Oldest uncompressed');
  my @col = (th('TOTAL'),
             &renderColumns($sum{jfs},$sum{jfsfs},$COLOR{Scality},1),
             &renderColumns($sum{dm11c},$sum{dm11cfs},$COLOR{dm11},1),
             &renderColumns($sum{dm11u},$sum{dm11ufs},$COLOR{'dm11 uncompressed'},1),
             &renderColumns($sum{otheru},$sum{otherufs},$COLOR{'other uncompressed'},1),
             td(['']));
  my $title = 'LSM location by family';
  if (param('other')) {
    $title = 'Imagery location (excluding LSMs)';
  }
  elsif (param('all')) {
    $title .= ' for Workstation-managed imagery';
  }
  $panel{image} =
        h3($title)
        . table({class => 'sortable',&identify('standard')},
                thead(Tr(th([@header]))),
                tbody(map {Tr(@$_)} @row),
                tfoot(Tr(@col))
               );
  $panel{countpie} = &generateSimplePieChart(hashref => \%count,
                                             title => '# Images per storage tier',
                                             color => \@COLOR,
                                             content => 'countpie',
                                             width => '600px', height => '400px');
  $size{$_} = sprintf '%.2f',$size{$_}/(1024**4) foreach (sort keys %size);
  $panel{sizepie} = &generateSimplePieChart(hashref => \%size,
                                            title => 'Storage footprint per storage tier',
                                            unit => 'TB',
                                            color => \@COLOR,
                                            content => 'sizepie',
                                            width => '600px', height => '400px');
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
