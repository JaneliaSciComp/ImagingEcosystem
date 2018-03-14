#!/usr/bin/perl

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
our $APPLICATION = 'Image processing completion';
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
my $MONGO = 1;
my $Session;
# Database
our ($dbh,$dbhw);

# ****************************************************************************
my $RUNMODE = ('apache' eq getpwuid($<)
              || 'daemon' eq getpwuid($<)) ? 'web' : 'command';

my ($change_basedate,$change_stopdate) = ('')x2;
if ($RUNMODE eq 'web') {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
  $change_basedate = param('start') if (param('start'));
  $change_stopdate = param('stop') if (param('stop'));
}
else {
GetOptions('start=s' => \$change_basedate,
           'stop=s'     => \$change_stopdate,
           help         => \my $HELP)
  or pod2usage(-1);
}
# Adjust parms if necessary
my($mm,$yy) = (localtime())[4,5];
my $BASEDATE = sprintf '%4d-%02d-%02d',$yy+1900,$mm+1,1;
$BASEDATE = $change_basedate if ($change_basedate);
my $today = UnixDate("today","%Y-%m-%d");
my $STOPDATE = UnixDate(DateCalc($today,'- 1 day'),"%Y-%m-%d");
$STOPDATE = $change_stopdate if ($change_stopdate);
$STOPDATE = $BASEDATE if ($STOPDATE lt $BASEDATE);
my $SUBTITLE = "All samples imaged $BASEDATE - $STOPDATE";
my $TERM = ($BASEDATE eq $STOPDATE)
  ? "WHERE DATE(i.create_date)='$BASEDATE'"
  : "WHERE DATE(i.create_date) BETWEEN '$BASEDATE' AND '$STOPDATE'";
my %sth = (
tmog => "SELECT line,ip1.value,ip2.value,name,i.create_date FROM image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id AND ip1.type='slide_code') JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND ip2.type='data_set') $TERM GROUP BY 1,2,3",
# -----------------------------------------------------------------------------
WS_entity => "SELECT e.id,name,ed3.value FROM entity e JOIN entityData ed1 ON (e.id=ed1.parent_entity_id AND ed1.entity_att='Line' AND ed1.value=?) JOIN entityData ed2 ON (e.id=ed2.parent_entity_id AND ed2.entity_att='Slide Code' AND ed2.value=?) LEFT OUTER JOIN entityData ed3 ON (e.id=ed3.parent_entity_id AND ed3.entity_att='Status') WHERE entity_type='Sample' AND name NOT LIKE '%-Retired' AND name NOT LIKE '%~%' ORDER BY e.id DESC",
);



# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayCompletionStatus();
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
  &printHeader() if ($RUNMODE eq 'web');
  $sth{tmog}->execute();
  my $ar = $sth{tmog}->fetchall_arrayref();
  my(%complete,%discover,%incomplete,%line,%tmog,%unknown);
  my @all;
  foreach (@$ar) {
    # Line, slide code, data set, tmog date
next if ($MONGO && $_->[0] eq 'Not_Applicable');
    $tmog{$_->[2]}++;
    $line{$_->[2]}{$_->[0]}++;
    $tmog{TOTAL}++;
    $line{TOTAL}{$_->[0]}++;
    my $ar2;
    if ($MONGO) {
      my $rest = $CONFIG{'jacs'}{url}.$CONFIG{'jacs'}{query}{SampleImageSearch} . "?line=$_->[0]&slideCode=$_->[1]";
      my $response = get $rest;
      &terminateProgram("<h3>REST GET returned null response</h3>"
                        . "<br>Request: $rest<br>") unless (length($response));
      my $rvar;
      eval {$rvar = decode_json($response)};
      &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                        . "Response: $response<br>Error: $@") if ($@);
      &terminateProgram("<h3>REST GET returned null</h3><br>Request: $rest")
        unless (scalar @$rvar);
      push @$ar2,[@{$_}{qw(_id name status)}] foreach (@$rvar);
    }
    else {
      $sth{entity}->execute($_->[0],$_->[1]);
      $ar2 = $sth{entity}->fetchall_arrayref();
    }
    # Entity ID, name, status
    if (scalar @$ar2) {
      my $status = $ar2->[0][2];
      push @all,[@$_,$ar2->[0][1],$status||''];
      if ($status && ($status eq 'Complete')) {
        $complete{$_->[2]}++;
        $complete{TOTAL}++;
      }
      else {
        $discover{$_->[2]}++;
        $discover{TOTAL}++;
        $incomplete{$status}++;
      }
    }
    else {
      push @all,[@$_,('')x2];
      $unknown{$_->[2]}++;
      $unknown{TOTAL}++;
    }
  }
  my $html = '';
  $complete{TOTAL} ||= 0;
  $discover{TOTAL} ||= 0;
  $unknown{TOTAL} ||= 0;
  if ($RUNMODE eq 'web') {
    my $hp;
    $hp = "['".(($complete{TOTAL}) ? 'Complete' : '')."',$complete{TOTAL}],"
          . "['".(($discover{TOTAL}) ? 'In process' : '')."',$discover{TOTAL}],"
          . "['".(($unknown{TOTAL}) ? 'Not started' : '')."',$unknown{TOTAL}],";
    my @row;
    foreach (sort keys %tmog) {
      next if ($_ eq 'TOTAL');
      push @row,Tr(td($_),
                   td({style => 'text-align: center'},[scalar keys %{$line{$_}},$tmog{$_},$unknown{$_},$discover{$_},$complete{$_}]));
    }
    my @pie_arr;
    push @pie_arr,[$_||'(none)',$incomplete{$_}]
      foreach (sort keys %incomplete);
    $html = 
            h1({style => 'text-align: center'},
               "Image processing completion status") .
            h3({style => 'text-align: center'},$SUBTITLE) . br .
            div({style => 'float: left'},
                div({style => 'margin-top: 10px; position: relative'},
                    table({class => 'summary'},
                      Tr(td(['Unique lines imaged',scalar keys %{$line{TOTAL}}])),
                      Tr(td(['Samples imaged',$tmog{TOTAL}])),
                      Tr(td(['Awaiting Workstation discovery',$unknown{TOTAL}])),
                      Tr(td(['On Workstation (pending processing)',$discover{TOTAL}])),
                      Tr(td(['On Workstation (complete)',$complete{TOTAL}]))),
                    &createExportFile(\@all,"_completion",
                                      ['Line','Slide code','Data set','tmog date','Sample','Status'])
                   ),
                  &halfPie($hp,'Completion<br>status','total'),
                  ((scalar @pie_arr)
                   ? &pieChart(\@pie_arr,'Samples pending processing','pie1')
                   : '')
               ) .
            div({style => 'float: left'},
                table({id => 'stats',class => 'tablesorter standard'},
                       thead(Tr(th(['Data set','Lines','Samples','Awaiting discovery','Pending processing','Complete']))),
                       tbody(@row),
                       tfoot(Tr({style => 'text-align: ccenter'},
                                th('TOTAL'),
                                th({style => 'text-align: center'},
                                   [scalar keys %{$line{TOTAL}},$tmog{TOTAL},
                                    $unknown{TOTAL},$discover{TOTAL},
                                    $complete{TOTAL}])))));
    $html .= div({style => 'clear: both'},'');
    print $html,end_form,&sessionFooter($Session),end_html;
  }
#  else {
#    print join("\t",@CT_HEAD) . "\n";
#    push @row,[$ds,$count,(sprintf '%.2f%%',
#               $no_errors{$ds}/$count*100),mean($acc{$ds}),
#               stddev($acc{$ds})];
#    foreach (@row) {
#      print join("\t",@$_) . "\n";
#    }
#  }
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
