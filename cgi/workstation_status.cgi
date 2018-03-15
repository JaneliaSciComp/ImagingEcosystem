#!/bin/env perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Manip qw(DateCalc UnixDate);
use DBI;
use Getopt::Long;
use IO::File;
use JSON;
use LWP::Simple;
use POSIX qw(ceil strftime);
use Statistics::Basic qw(:all);
use XML::Simple;
use JFRC::Utils::DB qw(:all);
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
our $APPLICATION = 'Workstation LSM status';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";
my %CONFIG;
# Highcharts
my $gradient = <<__EOT__;
Highcharts.getOptions().colors = Highcharts.map(Highcharts.getOptions().colors, function (color) {
    return {radialGradient: {
              cx: 0.5,
              cy: 0.3,
              r: 0.6},
            stops: [
              [0, color],
              [1, Highcharts.Color(color).brighten(-0.3).get('rgb')]]};
});
__EOT__
my $pie_chart = <<__EOT__;
chart: {type: 'pie',
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
       },   
credits: {enabled: false},
__EOT__
my $pie_tooltip_plot = <<__EOT__;
tooltip: { pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>' },
plotOptions: { pie: { allowPointSelect: true,
                      cursor: 'pointer',
                      dataLabels: {enabled: true,
                                   color: '#789abc',
                                   connectorColor: '#789abc',
                                   formatter: function() {
                                     return '<b>'+ this.point.name +'</b>: '+ Highcharts.numberFormat(this.percentage,2) +' %';
                                   }
                                  }
                    }
             },
__EOT__


# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Web
our ($USERID,$USERNAME);
my $Session;
my $MONGO = 1;
# Database
our ($dbh,$dbhw);

# ****************************************************************************
my $RUNMODE = ('apache' eq getpwuid($<)
              || 'daemon' eq getpwuid($<)) ? 'web' : 'command';

my ($change_basedate,$change_stopdate) = ('')x2;
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
$change_basedate = param('start') if (param('start'));
$change_stopdate = param('stop') if (param('stop'));
# Adjust parms if necessary
my($mm,$yy) = (localtime())[4,5];
my $BASEDATE = sprintf '%4d-%02d-%02d',$yy+1900,$mm+1,1;
$BASEDATE = $change_basedate if ($change_basedate);
my $STOPDATE = UnixDate("today","%Y-%m-%d");
$STOPDATE = $change_stopdate if ($change_stopdate);
$STOPDATE = $BASEDATE if ($STOPDATE lt $BASEDATE);
my $SUBTITLE = "All LSMs imaged $BASEDATE - $STOPDATE";
my $TERM = ($BASEDATE eq $STOPDATE)
  ? "DATE(i.create_date)='$BASEDATE'"
  : "DATE(i.create_date) BETWEEN '$BASEDATE' AND '$STOPDATE'";
(my $TERMW = $TERM) =~ s/i.create_date/edt.value/g;
my %sth = (
tmog => "SELECT line,ip1.value AS slidecode,ip2.value AS dataset,name,i.create_date FROM image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id AND ip1.type='slide_code') JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND ip2.type='data_set') WHERE $TERM",
# -----------------------------------------------------------------------------
WS_status => "SELECT name,CONCAT(edl.value,'-',edsc.value),eds.value FROM entity e LEFT OUTER JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Status') JOIN entityData edt ON (e.id=edt.parent_entity_id AND edt.entity_att='TMOG Date') JOIN entityData edl ON (e.id=edl.parent_entity_id AND edl.entity_att='Line') JOIN entityData edsc ON (e.id=edsc.parent_entity_id AND edsc.entity_att='Slide Code') WHERE entity_type='Sample' AND $TERMW AND name NOT LIKE '%~%'",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayCompletionStatus();
# We're done!
if ($dbh) {
  $dbh->disconnect;
  $dbhw->disconnect unless ($MONGO);
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Get WS REST config
  my $file = DATA_PATH . 'rest_services.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %CONFIG = %$hr;

  # Connect to databases
  &dbConnect(\$dbh,'sage');
  &dbConnect(\$dbhw,'workstation') unless ($MONGO);
  foreach (keys %sth) {
    if (/^WS/) {
      unless ($MONGO) {
        (my $n = $_) =~ s/WS_//;
        $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
      }
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
  }
}


sub displayCompletionStatus
{
  # Build HTML
  &printHeader();
  # Populate status hash (slide code => status)
  my ($ar2,$rest);
  if ($MONGO) {
    $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{WorkstationStatus};
    if ($BASEDATE && $STOPDATE) {
      $rest .= "?startDate=$BASEDATE&endDate=$STOPDATE";
    }
    elsif ($BASEDATE) {
      $rest .= "?startDate=$BASEDATE";
    }
    elsif ($STOPDATE) {
      $rest .= "?endDate=$STOPDATE";
    }
    my $rvar = &getREST($rest);
    foreach (@$rvar) {
      push @$ar2,[$_->{name},join('-',@{$_}{qw(line slideCode)}),$_->{status}];
    }
  }
  else {
    $sth{status}->execute();
    $ar2 = $sth{status}->fetchall_arrayref();
  }
  my %samplel = map {$_->[1] => $_->[0]} @$ar2; # Sample -> line
  my %status = map {$_->[1] => $_->[-1]} @$ar2; # Sample -> status
  # Get SAGE information
  $sth{tmog}->execute();
  my $ar = $sth{tmog}->fetchall_arrayref();
  # Line, slide code, data set, name tmog date
  my(%complete,%discover,%incomplete,%line,%sample,%tmog,%unknown);
  my @all;
  foreach (@$ar) {
    $tmog{$_->[2]}++;
    $line{$_->[2]}{$_->[0]}++;
    $tmog{TOTAL}++;
    $line{TOTAL}{$_->[0]}++;
    my $samp = $samplel{join('-',$_->[0],$_->[1])} || '';
    $sample{$_->[2]}{$samp}++;
    $sample{TOTAL}{$samp}++;
    my $stat = $status{join('-',$_->[0],$_->[1])} || '';
    push @all,[$samp,@$_,$stat];
    if ($stat eq 'Complete') {
      $complete{$_->[2]}++;
      $complete{TOTAL}++;
    }
    elsif ($stat) {
      $discover{$_->[2]}++;
      $discover{TOTAL}++;
      $incomplete{$stat}++;
    }
    else {
      $unknown{$_->[2]}++;
      $unknown{TOTAL}++;
    }
  }
  my $html = '';
  $complete{TOTAL} ||= 0;
  $discover{TOTAL} ||= 0;
  $unknown{TOTAL} ||= 0;
  my $hp;
  $hp = "['".(($complete{TOTAL}) ? 'Complete' : '')."',$complete{TOTAL}],"
        . "['".(($discover{TOTAL}) ? 'In process' : '')."',$discover{TOTAL}],"
        . "['".(($unknown{TOTAL}) ? 'Not started' : '')."',$unknown{TOTAL}],";
  my @row;
  foreach (sort keys %tmog) {
    next if ($_ eq 'TOTAL');
    push @row,Tr(td($_),
                 td({style => 'text-align: center'},[scalar keys %{$line{$_}},scalar keys %{$sample{$_}},$tmog{$_},$unknown{$_},$discover{$_},$complete{$_}]));
  }
  my @pie_arr;
  push @pie_arr,[$_||'(none)',$incomplete{$_}]
    foreach (sort keys %incomplete);
  $html = h1({style => 'text-align: center'},
             "Workstation status") .
          h3({style => 'text-align: center'},$SUBTITLE) . br .
          div({style => 'float: left'},
              div({style => 'margin-top: 10px; position: relative'},
                  (($MONGO) ? img({src => '/images/mongodb.png'}) : ''),
                  table({class => 'summary'},
                    Tr(td(['Unique lines imaged',scalar keys %{$line{TOTAL}}])),
                    Tr(td(['Samples imaged',scalar keys %{$sample{TOTAL}}])),
                    Tr(td(['LSMs imaged',$tmog{TOTAL}])),
                    Tr(td(['Awaiting Workstation discovery',$unknown{TOTAL}])),
                    Tr(td(['On Workstation (in process)',$discover{TOTAL}])),
                    Tr(td(['On Workstation (complete)',$complete{TOTAL}]))),
                  &createExportFile(\@all,"_workstation_status",
                                    ['Sample','Line','Slide code','Data set','LSM','tmog date','Status'])
                 ),
              &halfPie($hp,'Completion<br>status','total'),
              ((scalar @pie_arr)
                ? &pieChart(\@pie_arr,'LSMs in process','pie1')
                : '')
             ) .
             div({style => 'float: left'},
                 table({id => 'stats',class => 'tablesorter standard'},
                        thead(Tr(th(['Data set','Lines','Samples','LSMs','Awaiting discovery','In process','Complete']))),
                        tbody(@row),
                        tfoot(Tr({style => 'text-align: ccenter'},
                                 th('TOTAL'),
                                 th({style => 'text-align: center'},
                                    [scalar keys %{$line{TOTAL}},scalar keys %{$sample{TOTAL}},$tmog{TOTAL},
                                     $unknown{TOTAL},$discover{TOTAL},
                                     $complete{TOTAL}])))));
    $html .= div({style => 'clear: both'},'');
    print $html,end_form,&sessionFooter($Session),end_html;
}



sub getREST
{
  my($rest) = @_;
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


sub halfPie
{
  my($hp_text,$title,$container) = @_;
  my $code = <<__EOT__;
<div id="$container" style="position: relative; width: 600px; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
Highcharts.setOptions({
  colors: ['#50B432','#DDDF00','#ED561B']
});
$gradient
\$('#total').highcharts({
  chart: {plotBackgroundColor: "#222",
          plotBorderWidth: 0,
          plotShadow: false},
  credits: {enabled: false},
  title: {text: '$title',
          align: 'center',
          style: { "color": "white"},
          verticalAlign: 'middle',
          y: -20},
  tooltip: {pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'},
  plotOptions: {pie: {dataLabels: {enabled: true,
                                   distance: -50,
                                   style: {fontWeight: 'bold',
                                           color: 'white',
                                           textShadow: '0px 1px 2px #666'}},
                      startAngle: -90,
                      size: "100%",
                      endAngle: 90,
//                      center: ['50%','75%']
                     }},
  series: [{type: 'pie',
            name: '$title',
            innerSize: '50%',
            data: [$hp_text]}
          ]});
});
</script>
__EOT__
}


sub pieChart
{
  my($ar,$title,$container) = @_;
  my $data;
  my @element = map { "['" . ($_->[0]||'(none)') . "'," . $_->[1] . ']'} @$ar;
  $data = "data: [\n" . join(",\n",@element) . "\n]";
  $a = <<__EOT__;
<div id="$container" style="width: 600px; height: 400px; margin: 0 auto"></div>
<script type="text/javascript">
\$(function () {
Highcharts.setOptions({
colors: ['#7cb5ec','#f7a35c','#8085e9','#f15c80','#e4d354','#2b908f','#f45b5b','#91e8e1']
});
$gradient
        \$('#$container').highcharts({
            $pie_chart
            title: { text: '$title',
                     style: { "color": "white"} },
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
                    ('highcharts-4.0.1/highcharts','jquery/jquery.tablesorter',
                     'tablesorter');
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
