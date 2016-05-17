#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use CGI::Carp qw(fatalsToBrowser);
use Data::Dumper;
use DBI;
use IO::File;
use JSON;
use LWP::Simple;
use POSIX qw(strftime);
use Switch;
use Time::HiRes qw(gettimeofday tv_interval);
use Time::Local qw(timelocal);
use JFRC::LDAP;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/opt/informatics/data/';
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";
my %CONFIG;

# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'BlockView';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my @COLOR = qw(
33ff33 ff3333 3333ff 33ffff ff33ff ffff33
33cc33 cc3333 3333cc 33cccc cc33cc cccc33
339933 993333 333399 339999 993399 999933
336633 663333 333366 336666 663366 666633
);
my %SELECTOR = (Annotator => 2,
                Microscope => 3,
                'Data set' => 4);
my %MSELECTOR = (Status     => 'status',
                 'Data set' => 'dataSet');

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
our ($dbh,$dbhw);
my %sth = (
IMAGES => "SELECT i.create_date,annotated_by,microscope,data_set,slide_code,line,area,i.name,i.id FROM image_data_mv i WHERE i.name LIKE '%lsm' ORDER BY DATE(i.create_date),2",
# ----------------------------------------------------------------------------
WS_SAMPLES => "SELECT DISTINCT edt.value,eds.value,edd.value,e.name,e.id FROM entity e JOIN entityData edt ON (e.id=edt.parent_entity_id AND edt.entity_att='TMOG Date') LEFT OUTER JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Status') JOIN entityData edd ON (e.id=edd.parent_entity_id AND edd.entity_att='Data Set Identifier') ORDER BY DATE(edt.value),2",
);

my $MONGO = 0;
my %block_color = (unknown => '999999');
our $service;
my $CLEAR = div({style=>'clear:both;'},NBSP);

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM,
                                expire     => '+24h');
&sessionLogout($Session) if (param('logout'));
our $USERID = $Session->param('user_id');
our $USERNAME = $Session->param('user_name');

# Parms
my $HEIGHT = param('height') || 150;
my $START = param('start') || '';
my $STOP = param('stop') || '';
my $SELECTOR = (param('entity') eq 'Samples') ? param('mselector') : param('selector');
# Initialize
&initializeProgram();

# ----- Page header -----
if (($Session->param('scicomp'))
    || ($Session->param('flylight_split_screen'))) {
  print &pageHead(),start_multipart_form;
  (param('choose')) ? &showResults() : &showUserDialog();
}
else {
  &terminateProgram('Access to this program is restricted to the Fly Light '
                    . 'Steering Committee and Fly Light staff.');
}
# ----- Footer -----
print div({style => 'clear: both;'},NBSP),end_form,
      &sessionFooter($Session),end_html;

# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
  $dbhw->disconnect;
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

  # Modify statements
  if ($START && $STOP) {
    $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) BETWEEN '$START' AND '$STOP' AND /;
    $sth{WS_SAMPLES} =~ s/ORDER /WHERE DATE(edt.value) BETWEEN '$START' AND '$STOP' ORDER /;
  }
  elsif ($START) {
    $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) >= '$START' AND /;
    $sth{WS_SAMPLES} =~ s/ORDER /WHERE DATE(edt.value) >= '$START' ORDER /;
  }
  elsif ($STOP) {
    $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) <= '$STOP' AND /;
    $sth{WS_SAMPLES} =~ s/ORDER /WHERE DATE(edt.value) <= '$STOP' ORDER /;
  }
  $sth{IMAGES} =~ s/2$/$SELECTOR{$SELECTOR}/;
  $sth{WS_SAMPLES} =~ s/2$/1/ if ($MSELECTOR{$SELECTOR} eq 'Data set');;
  # Connect to databases
  &dbConnect(\$dbh,'sage')
    || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
  &dbConnect(\$dbhw,'workstation')
    || &terminateProgram("Could not connect to Workstation: ".$DBI::errstr);
  foreach (keys %sth) {
    if (/^WS_/) {
      (my $n = $_) =~ s/WS_//;
      $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbhw->errstr);
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
    }
  }
  # Set up LDAP service
  $service = JFRC::LDAP->new();
}


sub showUserDialog()
{
  print div({class => 'boxed'},
            table({class => 'basic'},&dateDialog(),
                  Tr(td(['Report on:',
                         popup_menu(&identify('entity'),
                                    -values => ['LSMs','Samples'],
                                    -onChange => 'toggleReport();'
)])),
                  Tr({&identify('selector_row')},td(['Sort by:',
                         popup_menu(&identify('selector'),
                                    -values => [sort keys %SELECTOR],
                                    -default => 'Annotator')])),
                  Tr({&identify('mselector_row')},td(['Sort by:',
                         popup_menu(&identify('mselector'),
                                    -values => [sort keys %MSELECTOR],
                                    -default => 'Status')])),
                 ),
            &submitButton('choose','Search')),
            hidden(&identify('mongo'),default=>param('mongo')),
            hidden(&identify('performance'),default=>param('performance'));
}


sub dateDialog
{
  my $ago = strftime "%F",localtime (timelocal(0,0,12,(localtime)[3,4,5])-(60*60*24*30));
  (Tr(td('Start TMOG date:'),
      td(input({&identify('start'),
                value => $ago}) . ' (optional)')),
   Tr(td('Stop TMOG date:'),
      td(input({&identify('stop')}) . ' (optional)')));
}


sub showResults
{
  my $ar;
  my $entity = param('entity');
  my ($performance,$rest);
  if ($entity eq 'Samples') {
    if ($MONGO) {
      my $t0 = [gettimeofday];
      $rest = $CONFIG{url}.$CONFIG{query}{Blockview};
      if ($START && $STOP) {
        $rest .= "?startDate=$START&endDate=$STOP";
      }
      elsif ($START) {
        $rest .= "?startDate=$START";
      }
      elsif ($STOP) {
        $rest .= "?endDate=$STOP";
      }
      my $response = get $rest;
      &terminateProgram("<h3>REST GET returned null response</h3>"
                        . "<br>Request: $rest<br>")
        unless (length($response));
      $performance .= sprintf "REST GET: %.2fsec<br>",tv_interval($t0,[gettimeofday]);
      $t0 = [gettimeofday];
      my $rvar;
      eval {$rvar = decode_json($response)};
      &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                        . "Response: $response<br>Error: $@") if ($@);
      $performance .= sprintf "JSON decode: %.2fsec<br>",tv_interval($t0,[gettimeofday]);
      $t0 = [gettimeofday];
      my $index = $MSELECTOR{$SELECTOR};
      foreach (sort {$a->{tmogDate} cmp $b->{tmogDate}
                     || $a->{$index} cmp $b->{$index}} @$rvar) {
        push @$ar,[$_->{tmogDate},$_->{status},$_->{dataSet},$_->{name},$_->{'_id'}];
      }
      $performance .= sprintf "Remapping: %.2fsec for %d rows<br>",tv_interval($t0,[gettimeofday]),scalar(@$rvar);
    }
    else {
      my $t0 = [gettimeofday];
      $sth{SAMPLES}->execute();
      $ar = $sth{SAMPLES}->fetchall_arrayref();
      $performance .= sprintf "SQL query: %.2fsec for %d rows<br>",tv_interval($t0,[gettimeofday]),scalar(@$ar);
    }
    $block_color{Complete} = shift @COLOR;
    $block_color{Error} = shift @COLOR;
  }
  else {
    my $t0 = [gettimeofday];
    $sth{IMAGES}->execute();
    $ar = $sth{IMAGES}->fetchall_arrayref();
    $performance .= sprintf "SQL query: %.2fsec for %d rows<br>",tv_interval($t0,[gettimeofday]),scalar(@$ar);
  }
  unless ($ar && scalar(@$ar)) {
    my $msg = 'No imagery was found';
    $msg .= "<br>REST call: $rest" if ($MONGO);
    print &bootstrapPanel('No imagery found',
                          $msg,'danger');
    return;
  }
  my %key = ();
  my ($date,$date_section,$html) = ('')x3;
  my $count = 0;
  my @HEAD = ($entity eq 'Samples')
    ? ('TMOG date','Status','Data set','Name')
    : ('TMOG date','Annotator','Microscope','Data set','Slide code','Line','Area','Name');
  print div({&identify('display_section')},
            div({&identify('display')},''));
  print div({style => 'height: 210px'},''),
        &createExportFile($ar,'_blockview',\@HEAD),hr;
  foreach my $i (@$ar) {
    # create_date, annotator, microscope data_set, slide_code, line, area, name, id
    # tmog_date, status, data_set, name, id
    (my $idate = $i->[0]) =~ s/[ T].+//;
    if ($date && ($date ne $idate)) {
      $html .= &showDate($date,$count,\%key,$date_section);
      $date = $idate;
      $date_section = '';
      $count = 0;
      %key = ();
    }
    elsif (!$date) {
      $date = $idate;
    }
    my($bk,$bv,$bhtml) = ($entity eq 'Samples') ? &singleSample(@$i) : &singleImage(@$i);
    $key{$bk} = $bv;
    $date_section .= $bhtml;
    $count++;
  }
  $html .= &showDate($date,$count,\%key,$date_section);
  $html .= (br)x5;
  if (param('performance')) {
    $performance = div({class => 'boxed'},h2('Performance'),$performance) . br;
  }
  else {
    $performance = '';
  }
  print $performance . div({&identify('scrollarea')},$html),
}


sub showDate
{
  my($date,$count,$kr,$date_section) = @_;
  my $entity = (param('entity') eq 'Samples') ? 'samples' : 'images';
  div({style => 'float: left;'},
      span({style => 'font-size: 14pt'},$date)
      . " ($count $entity)" . br
      . join((NBSP)x4,map {$kr->{$_}} sort keys %$kr) . br
      . div({class => 'date_area'},$date_section))
  . $CLEAR;
}


sub singleSample
{
  my($tmog_date,$status,$dataset,$name,$id) = @_;
  $status ||= 'unknown';
  my $index = ($SELECTOR eq 'Data set') ? $dataset : $status;
  $block_color{$index} = shift @COLOR unless (exists $block_color{$index});
  my $loc = "sample_search.cgi?sample_id=$name";
  my $block = div({&identify($id),
                   class => 'iblock',
                   style => "background-color: #$block_color{$index}",
                   onmouseover => "showSampleDetail('$id','#$block_color{$index}');",
                   onmouseout => "noDetail()",
                   onclick => "navigate('$loc')"
                  },'');
  my $background = &setBackground($block_color{$index});
  my $value = span({style => "color: #$block_color{$index}; $background"},$index);
  return($index,$value,$block);
}


sub singleImage
{
  my($create_date,$annotator,$microscope,$dataset,$slide_code,$line,$area,$name,$id) = @_;
  $annotator ||= 'unknown';
  $dataset ||= 'unknown';
  $microscope ||= 'unknown';
  my $index = $annotator;
  switch ($SELECTOR) {
    case 'Data set'   { $index = $dataset }
    case 'Microscope' { $index = $microscope }
  }
  $block_color{$index} = shift @COLOR unless (exists $block_color{$index});
  my $loc = "view_sage_imagery.cgi?_op=stack&_image=$name";
  my $block = div({&identify($id),
                   class => 'iblock',
                   style => "background-color: #$block_color{$index}",
                   onmouseover => "showDetail($id,'$create_date','$name','$annotator','$microscope','$dataset','$slide_code','$line','$area','#$block_color{$index}');",
                   onmouseout => "noDetail()",
                   onclick => "navigate('$loc')"
                  },'');
  my $background = &setBackground($block_color{$index});
  my $value = span({style => "color: #$block_color{$index}; $background"},$index);
  return($index,$value,$block);
}


sub setBackground
{
  my $fg = shift;
  my $bg = ($fg =~ /3333/ || ($fg =~ /33/ && $fg !~ /[cf]/))
    ? 'background-color: #ccc' : '';
  return($bg);
}


sub submitButton
{
  my($id,$text) = @_;
  div({align => 'center'},
      submit({&identify($id),
              class => 'btn btn-success',
              value => $text}));
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d%H:%M:%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  my $len = scalar @{$ar->[0]} - 2;
  print $handle join("\t",@$head) . "\n";
  print $handle join("\t",@{$_}[0..$len]) . "\n" foreach (@$ar);
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export data");
  return($link);
}


# ****************************************************************************
# * Subroutine:  pageHead                                                    *
# * Description: This routine will return the page header.                   *
# *                                                                          *
# * Parameters:  Named parameters                                            *
# *              title: page title                                           *
# * Returns:     HTML                                                        *
# ****************************************************************************
sub pageHead
{
  my %arg = (title => $APPLICATION,
             @_);
  my %load = ();
  $load{load} = "tooltipInitialize();";
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    (qw(jquery/jquery-ui-latest jquery.jeditable.min),$PROGRAM);
  my @styles = Link({-rel=>'stylesheet',
                     -type=>'text/css',-href=>'https://code.jquery.com/ui/1.11.4/themes/ui-darkness/jquery-ui.css'});
  &standardHeader(title       => $arg{title},
                  css_prefix  => $PROGRAM,
                  script      => \@scripts,
                  style       => \@styles,
                  breadcrumbs => \@BREADCRUMBS,
                  expires     => 'now',
                  bootstrap   => '3.3.6',
                  jquery      => '1.12.0',
                  %load);
}
