#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use CGI::Carp qw(fatalsToBrowser);
use Data::Dumper;
use DBI;
use IO::File;
use JFRC::LDAP;
use JSON;
use LWP::Simple;
use POSIX qw(strftime);
use REST::Client;
use Time::HiRes qw(gettimeofday tv_interval);
use Time::Local qw(timelocal);
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/opt/informatics/data/';
use constant NBSP => '&nbsp;';
my %CONFIG;

# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = '20x screen review';
my $BASE = "/var/www/html/output/";
my $FLYSTORE_HOST = 'http://flystore.int.janelia.org';
# $FLYSTORE_HOST = 'http://django-dev:4000';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my @CROSS = qw(Polarity MCFO Stabilization);
my $STACK = 'view_sage_imagery.cgi?_op=stack;_family=split_screen_review;_image';
my $WEBDAV = 'http://jacs-webdav.int.janelia.org/WebDAV';
my $PRIMARY_MIP = 'Signal 1 MIP';
my $SECONDARY_MIP = 'Signal MIP ch1';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
my $MONGO = 0;
our ($dbh,$dbhf,$dbhw);
my %sth = (
AD_DBD => "SELECT MAX(ad.name),MAX(dbd.name) FROM line_relationship_vw lr "
           . "LEFT OUTER JOIN line_property_vw ad ON "
           . "(lr.object=ad.name AND lr.relationship='child_of' AND "
           . "ad.value='Split_GAL4-AD' AND ad.type='flycore_project') "
           . "LEFT OUTER JOIN line_property_vw dbd ON (lr.object=dbd.name "
           . "AND lr.relationship='child_of' AND dbd.value='Split_GAL4-DBD' "
           . "AND dbd.type='flycore_project') WHERE lr.subject=?",
DATASET => "SELECT DISTINCT value FROM image_vw i JOIN image_property_vw ip "
           . "ON (i.id=ip.image_id AND ip.type='data_set') WHERE i.line=? "
           . "AND i.family LIKE '%screen_review'",
HALVES => "SELECT value,name FROM line_property_vw lp JOIN line_relationship_vw lr "
          . "ON (lr.object=lp.name AND lr.relationship='child_of') WHERE "
          . "type='flycore_project' AND lr.subject=?",
IMAGEL => "SELECT i.name FROM image_vw i LEFT OUTER JOIN image_property_vw ipd "
          . "ON (i.id=ipd.image_id AND ipd.type='data_set') WHERE i.line=?",
IMAGES => "SELECT line,i.name,data_set,slide_code,area,cross_barcode,lpr.value AS requester,channel_spec,lsm_illumination_channel_1_power_bc_1,lsm_illumination_channel_2_power_bc_1,lsm_detection_channel_1_detector_gain,lsm_detection_channel_2_detector_gain,im.url,la.value,DATE(i.create_date) FROM image_data_mv i JOIN image im ON (im.id=i.id) LEFT OUTER JOIN line_property_vw lpr ON (i.line=lpr.name AND lpr.type='flycore_requester') JOIN line l ON (i.line=l.name) LEFT OUTER JOIN line_annotation la ON (l.id=la.line_id AND la.userid=?) WHERE data_set LIKE ? AND line LIKE 'JRC_IS%' ORDER BY 1",
SIMAGES => "SELECT i.name,area,im.url,lsm_illumination_channel_1_power_bc_1,lsm_illumination_channel_2_power_bc_1,lsm_detection_channel_1_detector_gain,lsm_detection_channel_2_detector_gain,channel_spec,data_set,objective,DATE(i.create_date) FROM image_data_mv i JOIN image im ON (im.id=i.id) WHERE line=? AND data_set LIKE ? ORDER BY slide_code,area",
SSCROSS => "SELECT line,cross_type FROM cross_event_vw WHERE line LIKE "
           . "'JRC\_SS%' AND cross_type LIKE 'Split%' GROUP BY 1,2",
ROBOT => "SELECT robot_id FROM line_vw WHERE name=?",
USERLINES => "SELECT value,COUNT(DISTINCT line) FROM image_vw i JOIN image_property_vw ip "
             . "ON (i.id=ip.image_id AND ip.type='data_set') WHERE value LIKE "
             . "'%screen_review' GROUP BY 1",
# ----------------------------------------------------------------------------
FB_ONROBOT => "SELECT Stock_Name,Production_Info,On_Robot FROM StockFinder "
              . "WHERE Stock_Name LIKE 'JRC_SS%'",
# ----------------------------------------------------------------------------
WS_LSMMIPS => "SELECT eds.entity_att,eds.value FROM entity e "
              . "JOIN entityData eds ON (e.id=eds.parent_entity_id) WHERE e.name=?",
);
our $service;
my $CLEAR = div({style=>'clear:both;'},NBSP);
my (%BRIGHTNESS,%DISCARD,%GAIN,%ONORDER,%PERMISSION,%POWER,%SSCROSS,%USERNAME);
my (%DATA_SET,%MISSING_MIP);
my @performance;
my $ACCESS = 0;
my $split_name = '';

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $t0 = [gettimeofday];
my $Session = &establishSession(css_prefix => $PROGRAM,
                                expire     => '+24h');
&sessionLogout($Session) if (param('logout'));
our $USERID = $Session->param('user_id');
our $USERNAME = $Session->param('user_name');

# Parms
my $HEIGHT = param('height') || 150;
my $VIEW_ALL = (($Session->param('scicomp'))
                || ($Session->param('flylight_split_screen')));
my $ALL_STABLE = $Session->param('scicomp') || 0;
my $RUN_AS = ($VIEW_ALL && param('_userid')) ? param('_userid') : '';
my $CAN_ORDER = ($VIEW_ALL) ? 0 : 1;
$CAN_ORDER = 1 if ($USERID eq 'dicksonb');
$CAN_ORDER = 1 if ($USERID eq 'svirskasr' && $RUN_AS);
$CAN_ORDER = 0 if ($USERID eq 'dolanm' || $RUN_AS eq 'dolanm');
my $START = param('start') || '';
my $STOP = param('stop') || '';
my $ALL_20X = param('all20x') || 0;
# Initialize
&initializeProgram();
#delete $PERMISSION{svirskasr};
($ACCESS,$CAN_ORDER,$VIEW_ALL) = (1,1,1)
  if (exists $PERMISSION{$USERID});

# ----- Page header -----
print &pageHead(),start_multipart_form;
if (param('request')) {
  if ($CAN_ORDER) {
    &requestCrosses();
  }
  else {
    print &bootstrapPanel('Did not place order','No crosses were ordered',
                          'warning');
  }
}
elsif (param('verify')) {
  &verifyCrosses();
}
elsif (param('choose')) {
  &chooseCrosses();
}
elsif (param('line')) {
  &showLine(param('line'));
}
elsif ($VIEW_ALL && !$RUN_AS && !param('user') && !param('choose') && !$ACCESS) {
  &limitFullSearch();
}
else {
  &showUserDialog();
}
# ----- Footer -----
print div({style => 'clear: both;'},NBSP),end_form,
      &sessionFooter($Session),end_html;

# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
  $dbhf->disconnect;
  $dbhw->disconnect unless ($MONGO);
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
  }
  elsif ($START) {
    $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) >= '$START' AND /;
  }
  elsif ($STOP) {
    $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) <= '$STOP' AND /;
  }
  print STDERR "Primary query: $sth{IMAGES}\n";
  # Connect to databases
  &dbConnect(\$dbh,'sage')
    || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
  &dbConnect(\$dbhf,'flyboy')
    || &terminateProgram("Could not connect to FlyBoy: ".$DBI::errstr);
  unless ($MONGO) {
    &dbConnect(\$dbhw,'workstation')
      || &terminateProgram("Could not connect to Workstation: ".$DBI::errstr);
  }
#  $dbhw = DBI->connect('dbi:mysql:dbname=flyportal;host=val-db',('flyportalRead')x2,{RaiseError=>1,PrintError=>0});
  foreach (keys %sth) {
    if (/^FB_/) {
      (my $n = $_) =~ s/FB_//;
      $sth{$n} = $dbhf->prepare($sth{$_}) || &terminateProgram($dbhf->errstr);
    }
    elsif (/^WS_/) {
      unless ($MONGO) {
        (my $n = $_) =~ s/WS_//;
        $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbhw->errstr);
      }
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
    }
  }
  # Set up LDAP service
  $service = JFRC::LDAP->new();
  # Get user permissions
  $file = DATA_PATH . $PROGRAM . '.json';
  open SLURP,$file
    or &terminateProgram("Can't open $file: $!");
  sysread SLURP,$slurp,-s SLURP;
  close(SLURP);
  $hr = decode_json $slurp;
  %PERMISSION = %$hr;
  push @performance,sprintf 'Initialization: %.4f sec',tv_interval($t0,[gettimeofday]);
}


sub showUserDialog()
{
  print div({class => 'boxed'},
            table({class => 'basic'},&dateDialog()),
            &submitButton('choose','Search')),br,
           hidden(&identify('_userid'),default=>param('_userid')),
           hidden(&identify('mongo'),default=>param('mongo'));
}


sub showLine
{
  my $line = shift;
  my @image;
  $sth{IMAGEL}->execute($line);
  my $ar = $sth{IMAGEL}->fetchall_arrayref();
  foreach (@$ar) {
    (my $wname = $_->[0]) =~ s/.+\///;
    $wname =~ s/\.bz2//;
    $wname .= '.bz2';
    my($signalmip,undef) = &getSingleMIP($wname);
    push @image,img({src => $WEBDAV . $signalmip});
  }
  print div({align => 'center',
             style => 'padding: 20px 0 20px 0; background-color: #111;'},@image);
}


sub getSingleMIP
{
  my($wname) = shift;
  if ($MONGO) {
    my $rest = $CONFIG{url}.$CONFIG{query}{LSMImages} . "?name=$wname";
    my $response = get $rest;
    return('','') unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    return($rvar->{files}{$PRIMARY_MIP}||$rvar->{files}{$SECONDARY_MIP}||'',$rvar->{brightnessCompensation}||0);
  }
  else {
    $sth{LSMMIPS}->execute($wname);
    my $hr = $sth{LSMMIPS}->fetchall_hashref('entity_att');
    return($hr->{$PRIMARY_MIP}{value},$hr->{'Brightness Compensation'}{value});
  }
}


sub limitFullSearch
{
  $sth{USERLINES}->execute();
  my $ar = $sth{USERLINES}->fetchall_arrayref();
  my %label = map {$a = (split('_',$_->[0]))[0];
                   my $user = $service->getUser($a);
                   $a => &getUsername($a) . " ($_->[1] lines)"} @$ar;
  $label{''} = '(Any)';
  my %screen_count;
  $screen_count{(split('_',$_->[0]))[1]} += $_->[1] foreach (@$ar);
  my $type = {'' => '(Any)',
              split => "Split screen ($screen_count{split} lines)",
              ti => "Terra incognita ($screen_count{ti} lines)"};
  print div({class => 'boxed'},
            table({class => 'basic'},
                  Tr(td('User:'),
                     td(popup_menu(&identify('user'),
                                   -values => [sort keys %label],
                                   -labels => \%label))),
                  Tr(td('Image type:'),
                     td(popup_menu(&identify('type'),
                                   -values => ['','split','ti'],
                                   -labels => $type))),
                  &dateDialog(),
                 ),
            &submitButton('choose','Search')),
        hidden(&identify('mongo'),default=>param('mongo')),br;
}


sub dateDialog
{
  my $ago = strftime "%F",localtime (timelocal(0,0,12,(localtime)[3,4,5])-(60*60*24*30));
  (Tr(td('Start TMOG date:'),
      td(input({&identify('start'),
                value => $ago}) . ' (optional)')),
   Tr(td('Stop TMOG date:'),
      td(input({&identify('stop')}) . ' (optional)'))),
   Tr(td('Grayscale:'),
      td(input({&identify('grayscale'),
                type => 'checkbox'})));
}


sub chooseCrosses
{
  my %lines = ();
  my ($adjusted,$class,$controls,$lhtml,$imagery,$last_line,$mcfo,$sss,
      $sss_adjusted,$polarity,$polarity_adjusted) = ('')x11;
  my $AUSER = $USERID;
  $AUSER = $RUN_AS || param('user') || $USERID if ($VIEW_ALL);
  print hidden({&identify('userid'),value => $AUSER});
  my $DSUSER = $USERID;
  my $DSTYPE = '%';
  if ($VIEW_ALL) {
    $DSUSER = $RUN_AS || param('user') || '%';
    $DSTYPE = param('type') if (param('type'));
  }
  my $ds = $DSUSER . '\_' . $DSTYPE . '\_screen\_review%';
  $t0 = [gettimeofday];
  $sth{IMAGES}->execute($AUSER,$ds);
  my $ar = $sth{IMAGES}->fetchall_arrayref();
  if ($ACCESS) {
    my @arr = @$ar;
    @$ar = ();
    my @list = @{$PERMISSION{$USERID}};
    foreach my $l (@arr) {
      next unless (grep(/$l->[2]/,@list));
      push @$ar,$l;
    }
  }
  push @performance,sprintf 'Primary query: %.4f sec',
                            tv_interval($t0,[gettimeofday]);
  unless (scalar @$ar) {
    print &bootstrapPanel('No screen imagery found',
                          'No screen imagery was found'
                          . (($DSUSER eq '%') ? '.' : " for $DSUSER")
                          . br . "Search terms: [$AUSER] [$ds]",'danger');
    return;
  }
  my $html = '';
  my($discarded,$ordered,$tossed) = (0)x3;
  &getFlyStoreOrders($ar);
  # Determine brightness
  my (%gain,%power);
  foreach my $l (@$ar) {
    my($power,$gain) = &getPowerGain($l->[7],@{$l}[8..11]);
    $power{$l->[0]}{$l->[4]} = $power;
    $gain{$l->[0]}{$l->[4]} = $gain;
  }
  foreach my $l (@$ar) {
    next unless ($l->[4] eq 'Brain');
    if ($power{$l->[0]}{Brain} > $power{$l->[0]}{VNC}) {
      $BRIGHTNESS{$l->[0]}{Brain} = 100 * ($power{$l->[0]}{VNC} / $power{$l->[0]}{Brain});
      $BRIGHTNESS{$l->[0]}{VNC} = 100;
    }
    elsif ($power{$l->[0]}{Brain} < $power{$l->[0]}{VNC}) {
      $BRIGHTNESS{$l->[0]}{Brain} = 100;
      $BRIGHTNESS{$l->[0]}{VNC} = 100 * ($power{$l->[0]}{Brain} / $power{$l->[0]}{VNC});
    }
  }
  my @export;
  my (%crossed,%cross_count);
  foreach my $l (@$ar) {
    # Line, image name, data set, slide code, area, cross barcode, requester,
    # channel spec, power 1, power 2, gain 1, gain 2, url, comment, TMOG date
    my($line,$name,$dataset,$slide,$area,$barcode,$requester,
       $chanspec,$power1,$power2,$gain1,$gain2,$url,$comment,$tmog_date) = @$l;
    my($power,$gain) = ($power{$line}{$area},$gain{$line}{$area});
    $lines{$line}++;
    $DATA_SET{$line} = $dataset;
    # Line control break
    if ($line ne $last_line) {
      $html .= &renderLine($last_line,$lhtml,$imagery,$adjusted,$mcfo,$sss,$sss_adjusted,$polarity,
                           $polarity_adjusted,$controls,$class,$tossed) if ($lhtml);
      $lhtml = &createLineHeader($line,$dataset,$barcode,$requester,$comment);
      $last_line = $line;
      ($class,$imagery,$adjusted,$tossed) = ('unordered','','',0);
      %crossed = ();
      my %cross_type = ();
      if ($barcode) {
        # Allow orders
        my %request;
        $request{$_} = $ONORDER{$line}{dateCreated}
          foreach @{$ONORDER{$line}{orders}};
        (my $stable_line = $line) =~ s/IS/SS/;
        my $sage_date = exists($SSCROSS{$stable_line});
        if ($sage_date) {
          $cross_type{MCFO} = $stable_line
            if (exists $SSCROSS{$stable_line}{SplitFlipOuts});
          $cross_type{Polarity} = $stable_line
            if (exists $SSCROSS{$stable_line}{SplitPolarity});
          $crossed{$_} = $cross_type{$_} foreach(qw(MCFO Polarity));
        }
        $request{lc($_)} && ($crossed{$_} = $stable_line) foreach (@CROSS);
        # Discard row
        my $discard = Tr(td({style => 'padding-left: 10px'},[
                            input({&identify(join('_',$line,'discard')),
                                   type => 'checkbox',
                                   class => 'lineselect',
                                   value => $barcode,
                                   onclick => 'tagCross("'.$line.'");'},'Discard'),'' ]));
        my $bump_ordered = 0;
        # Stable stock row
        my $stabilization;
        if ($sage_date) {
          $class = 'ordered';
          $bump_ordered = 1;
          $discarded = $tossed = 1 if ($DISCARD{$stable_line});
          my $link = a({href => "lineman.cgi?line=$stable_line",
                        target => '_blank'},
                       (($tossed) ? $stable_line : '(available)'),'');
          $stabilization = Tr(td({style => 'padding-left: 10px'},[
                                  (($tossed) ? "Stable stock $link is discarded"
                                             : "Stable stock $link"),
                                   '']));

          $crossed{Stabilization} = ($tossed) ? '' : $stable_line;
        }
        elsif (exists($request{stabilization})) {
          $class = 'ordered';
          $bump_ordered++;
          ($a = $request{stabilization}) =~ s/T.*//;
          $stabilization = Tr(td({style => 'padding-left: 10px'},[
                              "Stable stock (ordered $a)",'']));
        }
        else {
          $discard = '';
          $stabilization = &createStabilization($line,$barcode);
        }
        if ($tossed) {
          $controls = $stabilization;
        }
        else {
          my @CROSS2 = @CROSS[0..$#CROSS-1];
          $controls = div({class => 'checkboxes'},
                          table({style => 'margin-right: 10px'},
                                (map {my $c = join('_',$line,lc($_),'cross');
                                     if (exists $request{lc($_)}) {
                                       $class = 'ordered';
                                       $bump_ordered++;
                                       ($a = $request{lc($_)}) =~ s/T.*//;
                                       Tr(td({style => 'padding-left: 10px'},[
                                             $_,"Ordered $a"]))
                                     }
                                     else {
                                      Tr(td({style => 'padding-left: 10px'},[
                                      input({&identify($c),
                                             type => 'checkbox',
                                             class => 'lineselect',
                                             value => $barcode,
                                             onclick => 'tagCross("'.$line.'");'},$_
                                            . (($cross_type{$_})
                                               ? span({style => 'color: #4cc417'},
                                                      ' (cross exists)') : '')),
                                         input({&identify(join('_',$line,lc($_),'pri')),
                                                type => 'checkbox',
                                                class => 'lineselect'},'High priority')]))
                                     }
                                     } @CROSS2),
                                $stabilization,$discard));
        }
        $ordered++ if ($bump_ordered);
        if ($ALL_STABLE) {
#          ($mcfo) = &getStableImagery($stable_line,$DSUSER.'\_mcfo%');
          ($sss,$sss_adjusted) = &getStableImagery($stable_line,$DSUSER.'\_split\_screen\_review%');
          ($polarity,$polarity_adjusted) = &getStableImagery($stable_line,$DSUSER.'\_polarity%') unless ($sss);
        }
      }
      else {
        $controls = div({class => 'checkboxes',style => 'color: #000'},
          &bootstrapPanel('Cannot order stable splits',
                          (($barcode) ? "$line doesn't have two split halves"
                                      : "$line has no cross barcode"),'danger')
                       );
      }
    }
    $imagery .= &addSingleImage($line,$name,$area,$url,$power,$gain,'',$tmog_date);
    $adjusted .= &addSingleImage($line,$name,$area,$url,$power,$gain,'',$tmog_date,1)
      if (exists $BRIGHTNESS{$line}{$area});
    if ($area eq 'Brain') {
      $cross_count{Line}++;
      push @export,[(split('_',$dataset))[0],$line,$crossed{Polarity},
                    $crossed{MCFO},$crossed{Stabilization}];
      $crossed{$_} && ($cross_count{$_}++) foreach (@CROSS);
    }
  }
  $html .= &renderLine($last_line,$lhtml,$imagery,$adjusted,$mcfo,$sss,$sss_adjusted,$polarity,
                       $polarity_adjusted,$controls,$class,$tossed) if ($lhtml);

  push @performance,sprintf 'Main loop: %.4f sec',tv_interval($t0,
                                                              [gettimeofday]);
  my $export_button = '';
  if (scalar @export) {
    push @export,['TOTAL',@cross_count{'Line',@CROSS}];
    $export_button = &createExportFile(\@export,'_'.$USERID.'_screen_review',
                                       ['Annotator','Line',@CROSS]);
  }
  my $uname = $USERNAME;
  $uname .= " (running as $RUN_AS)" if ($RUN_AS);
  my @other = &createAdditionalData();
  print div({class => 'boxed',
             style => 'background-color: #cff'},
            h2('Performance data'),join(br,@performance),br)
    if ($USERID eq 'svirskasr');
  print div({class => 'boxed'},
            div({style => 'float: left'},
                table({class => 'standard'},
                      Tr(td(['User: ',$uname])),@other,
                      Tr(td(['Lines found: ',scalar keys %lines])),
                      (map { Tr(td(["$_ crosses requested:",
                                    div({class => lc($_).'_crosses'},0)])) } @CROSS),
                      Tr(td(['Discards requested:',
                             div({class => 'discards'},0)])),
                     )),
            &renderControls($ordered,scalar(keys %lines)-$ordered,$discarded,
                            $export_button),
            (($MONGO) ? img({src => '/images/mongodb.png'}) : ''),
            &submitButton('verify','Next >')),
        div({id => 'scrollarea'},$html),
        hidden(&identify('_userid'),default=>param('_userid'));
}


sub populateBrightness
{
  my($line,$ar,$index) = @_;
  foreach my $l (@$ar) {
    my($area) = $l->[$index];
    next unless ($area eq 'Brain');
    if ($POWER{$line}{Brain} > $POWER{$line}{VNC}) {
      $BRIGHTNESS{$line}{Brain} = 100 * ($POWER{$line}{VNC} / $POWER{$line}{Brain});
      $BRIGHTNESS{$line}{VNC} = 100;
    }
    elsif ($POWER{$line}{Brain} < $POWER{$line}{VNC}) {
      $BRIGHTNESS{$line}{Brain} = 100;
      $BRIGHTNESS{$line}{VNC} = 100 * ($POWER{$line}{Brain} / $POWER{$line}{VNC});
    }
  }
}


sub getPowerGain
{
  my($chanspec,$power1,$power2,$gain1,$gain2) = @_;
  return((!index($chanspec,'s')) ? ($power1,$gain1) : ($power2,$gain2));
}


sub createLineHeader
{
  my($line,$dataset,$barcode,$requester,$comment) = @_;
  my $split_halves = &getSplitHalves($line);
  my $type = ($dataset =~ /_ti_/) ? 'Terra incognita' : 'Split screen';
  my $lhtml = h3(&lineLink($line) . (NBSP)x5 . $type);
  my $bh = ($barcode) ? a({href => "/flyboy_search.php?cross=$barcode",
                           target => '_blank'},$barcode) : '';
  my @row = Tr(td(['Cross barcode:',$bh]));
  push @row,Tr(td(['Data set:',$dataset])) if ($VIEW_ALL);
  $requester = &getUsername((split('_',$dataset))[0])
    if ($VIEW_ALL && !param('user') && !$requester);
  push @row,Tr(td(['Requester:',$requester])) if ($requester);
  $comment ||= '';
  push @row,Tr(td(['Comment:',
                   ($CAN_ORDER) ? div({&identify($line.'_comment'),
                                       class => 'edit'},$comment)
                                : $comment]));
  $lhtml .= table({class => 'basic'},@row);
  $lhtml .= $split_halves if ($split_halves);
  return($lhtml);
}


sub getFlyStoreOrders
{
  my $ar = shift;
  my($is_lines,$discards);
  my $client = REST::Client->new();
  my $json = JSON->new->allow_nonref;
  foreach (@$ar) {
    push @$is_lines,$_->[0];
    ($a = $_->[0]) =~ s/IS/SS/;
    push @$discards,$a;
  }
  my $post_hash = {is_lines => $is_lines,discards => $discards};
  $client->POST("$FLYSTORE_HOST/api/orders/batch/",$json->encode($post_hash));
  if ($client->responseCode() != 200) {
    print &bootstrapPanel('FlyStore could not process IS/discard check',
                          Dumper($post_hash),'danger');
    &terminateProgram('Error response (' . $client->responseCode()
                      . ') from FlyStore for IS/discard check');
  }
  my $struct = $json->decode($client->responseContent());
  # Orders
  foreach my $order (keys %{$struct->{is_lines}}) {
    foreach my $ct (@{$struct->{is_lines}{$order}{crossTypes}}) {
      $ONORDER{$ct->{is_name}}{dateCreated} = $struct->{is_lines}{$order}{dateCreated};
      if (ref($ct->{cross_type}) eq 'ARRAY') {
         push @{$ONORDER{$ct->{is_name}}{orders}},$_ foreach (@{$ct->{cross_type}});
      }
      else {
        push @{$ONORDER{$ct->{is_name}}{orders}},$ct->{cross_type};
      }
    }
  }
  # Discards
  my $t0 = [gettimeofday];
  $sth{ONROBOT}->execute();
  my $ar2 = $sth{ONROBOT}->fetchall_arrayref();
  my @LIST = qw(Dead Exit Tossed);
  foreach (@$ar2) {
    $a = $_->[1] || '';
    if ($a) {
      $_->[2] ||= '';
      $DISCARD{$_->[0]}++ if (grep(/$a/,@LIST) && ($_->[2] ne 'Yes'));
    }
  }
  foreach my $order (keys %{$struct->{discards}}) {
    foreach my $l (@{$struct->{discards}{$order}{stockName}}) {
      $DISCARD{$l}++;
    }
  }
  push @performance,sprintf 'Discard hash build: %.4f sec',tv_interval($t0,[gettimeofday]);
  # Stable stocks on SAGE
  $t0 = [gettimeofday];
  $sth{SSCROSS}->execute();
  $ar = $sth{SSCROSS}->fetchall_arrayref();
  $SSCROSS{$_->[0]}{$_->[1]}++ foreach (@$ar);
  push @performance,sprintf 'Stable split cross hash build: %.4f sec',tv_interval($t0,[gettimeofday]);
}


sub getStableImagery
{
  my($line,$ds) = @_;
  my($img,$adjusted) = ('')x2;
  $ds =~ s/dicksonb/dicksonlab/;
  $sth{SIMAGES}->execute($line,$ds);
  my $ar = $sth{SIMAGES}->fetchall_arrayref();
  return('','') unless (scalar @$ar);
  my $used = 0;
  foreach my $i (@$ar) {
    my $objective = $i->[-2];
    next unless ($objective =~ /20[Xx]/);
    last if ((!$ALL_20X) && ($used >= 2));
    my($power,$gain) = &getPowerGain($i->[-3],@{$i}[3..6]);
    $POWER{$line}{$i->[1]} = $power;
    $GAIN{$line}{$i->[1]} = $gain;
    $used++;
  }
  &populateBrightness($line,$ar,1);
  $used = 0;
  foreach my $i (@$ar) {
    my $tmog_date = pop @$i;
    my $objective = pop @$i;
    next unless ($objective =~ /20[Xx]/);
    last if ((!$ALL_20X) && ($used >= 2));
    # Image name, area, url, power 1, power 2, gain 1, gain 2, channel spec, data set
    splice(@$i,3,6,$POWER{$line}{$i->[1]},$GAIN{$line}{$i->[1]},$i->[-1]);
    push @$i,$tmog_date;
    $img .= &addSingleImage('',@$i);
    $adjusted .= &addSingleImage($line,@$i,1)
      if (exists $BRIGHTNESS{$line}{$i->[1]});
    $used++;
  }
  return($img,$adjusted);
}


sub getSplitHalves
{
  my($line) = shift;
  $sth{HALVES}->execute($line);
  my $hr = $sth{HALVES}->fetchall_hashref('value');
  my $html = '';
  $split_name = '';
  if (scalar(keys %$hr)) {
    $html = join(br,table({class => 'halves'},
                          map {Tr(th($_.':'),td(&lineLink($hr->{$_}{name})))}
                              sort keys %$hr));
    $split_name = join('-x-',map {$hr->{$_}{name}} sort keys %$hr);
  }
  return($html);
}


sub lineLink
{
  my $l = shift;
  a({href => 'lineman.cgi?line='.$l,
     target => '_blank'},$l);
}


sub createStabilization
{
  my($line,$barcode) = @_;
  Tr(td({style => 'padding-left: 10px'},
        [input({&identify(join('_',$line,'stabilization_cross')),
                type => 'checkbox',
                class => 'lineselect',
                value => $barcode,
                onclick => 'tagCross("'.$line.'");'},'Stable stock'),'']));
}


sub addSingleImage
{
  my($line,$name,$area,$url,$power,$gain,$dataset,$tmog_date,$adjusted) = @_;
  $dataset ||= '';
  (my $wname = $name) =~ s/.+\///;
  $wname =~ s/\.bz2//;
  my($bc,$signalmip) = ('')x2;
  ($signalmip,$bc) = &getSingleMIP($wname);
  ($signalmip,$bc) = &getSingleMIP($wname.'.bz2') unless ($signalmip);
  return('') unless ($signalmip);
  $bc = 0 if ($tmog_date lt '2016-02-17');
  if ($bc) {
    foreach (split(',',$bc)) {
      if ($_ > 1) {
        $bc = $_ * 100;
        last;
      }
    }
    $bc = 100 if ($bc =~ /,/);
  }
  (my $signal = $signalmip) =~ s/signal.*png$/signal.mp4/;
  (my $i = $signalmip) =~ s/.+filestore\///;
  my $pga;
  if ($i) {
    my @parms = ();
    my $parms = '';
    $i = $WEBDAV . $i;
    my $style = '';
    if (param('grayscale')
        || ($adjusted && exists($BRIGHTNESS{$line}{$area}))) {
      $style = 'filter: ';
      my $fp;
      if (param('grayscale')) {
        $fp .= 'grayscale(100%) ';
        push @parms,'grayscale=1';
      }
      if ($adjusted && exists($BRIGHTNESS{$line}{$area})) {
        $fp .= "brightness($BRIGHTNESS{$line}{$area}%);";
        push @parms,"brightness=$BRIGHTNESS{$line}{$area}%";
        $pga = $BRIGHTNESS{$line}{$area};
      }
      $style .= $fp . '; -webkit-filter: ' . $fp;
      if (scalar(@parms)) {
        $parms = '&' . join('&',@parms);
      }
    }
    $BRIGHTNESS{$line}{$area} = $bc if ($bc);
    my $url2 = $WEBDAV . $signalmip;
    my $caption=$line;
    $caption .= " ($split_name)" if ($split_name);
    $signalmip = a({href => "view_image.cgi?url=$url2"
                            . "&caption=$caption" . $parms,
                    target => '_blank'},
                   img({style => $style,
                        src => $url2, height => $HEIGHT}));
  }
  (my $all = $signal) =~ s/signal.+mp4$/all.mp4/;
  $signal = a({href => $WEBDAV . $signal,
               target => '_blank'},
              img({src => '/images/stack_plain.png',
                   title => 'Show signal movie'}));
  $all = a({href => $WEBDAV . $all,
            target => '_blank'},
           img({src => '/images/stack_multi.png',
                title => 'Show reference+signal movie'}));
  if ($url) {
    $url =~ s/JSF/JFS/;
    $url = a({href => $url},
             img({src => '/images/lsm_image.png',
                  title => 'Download LSM'}));
  }
  else {
    $url = NBSP;
  }
  my $pgv = 'Unknown power/gain';
  my $format = "Power&times;Gain %.2f&times;%d (%.2f)";
  $pgv = sprintf $format,$power/100,$gain,($power/100)*$gain
    if ($power && $gain);
  if ($adjusted ) {
    $pgv = ($bc) ? (sprintf 'Brightness compensation (%.1f%%)',$bc)
                 : (sprintf 'Power/gain adjusted (%.1f%%)',$pga);
  }
  my $PREFIX = $STACK;
  if ($dataset =~ /mcfo/) {
    $PREFIX =~ s/split_screen_review/flylight_flip/;
  }
  if ($dataset =~ /polarity/) {
    $PREFIX =~ s/split_screen_review/flylight_polarity/;
  }
  my @row = ();
  my %opt = (class => 'imgoptions');
  ($url,$signal,$all,$opt{class}) = ('')x4 if ($adjusted);
  push @row,Tr(td({colspan => 5},$pgv));
  div({class => 'single_mip'},$signalmip,br,
      table(Tr(td({width => '14%'},$url),
               td({width => '14%'},NBSP),
               td({width => '44%'},a({href => "$PREFIX=$name",
                                      target => '_blank'},$area)),
               td({width => '14%',%opt},$signal),
               td({width => '14%'},$all)),
            @row
           )
     );
}


sub renderLine {
  my($line,$html,$imagery,$adjusted,$mcfo,$stable,$sadjusted,$polarity,$padjusted,$controls,$class,$tossed) = @_;
  $MISSING_MIP{$line}++ unless ($imagery);
  $imagery ||= div({class => 'stamp'},'No imagery available');
  $imagery = div({class => 'category initialsplit'},
                 span({style => 'padding: 0 60px 0 60px'},'Initial split'))
             . $imagery;
#  $imagery = div({style => 'float:left'},
#                 table({class => 'categoryx'},
#                       Tr(th({style => 'background-color: #058d95;'},
#                             'Initial split'),
#                          td($imagery)))
#                );
  $adjusted = $CLEAR
              . div({class => 'inputblock',style => 'height: 100%;'},
                    div({class => 'category initialsplit_adjusted'},
                        span({style => 'padding: 0 60px 0 60px'},'Adjusted'))
                    . $adjusted) if ($adjusted);
  $mcfo = $CLEAR
          . div({class => 'inputblock',style => 'height: 100%;'},
                div({class => 'category mcfo',
                     style => 'background-color: #553d95;'},
                    span({style => 'padding: 0 60px 0 60px'},'20x MCFO'))
                . $mcfo) if ($mcfo);
  if ($stable) {
    $stable = $CLEAR
              . div({class => 'inputblock',style => 'height: 100%;'},
                    div({class => 'category stablesplit'},
                        span({style => 'padding: 0 60px 0 60px'},'20x Stable'))
                    . $stable);
    $sadjusted = $CLEAR
                 . div({class => 'inputblock',style => 'height: 100%;'},
                       div({class => 'category stablesplit_adjusted'},
                           span({style => 'padding: 0 20px 0 20px'},'20x Stable adjusted'))
                       . $sadjusted) if ($sadjusted);
  }
  elsif ($polarity) {
    $stable = $CLEAR
              . div({class => 'inputblock',style => 'height: 100%;'},
                    div({class => 'category polarity',
                         id => 'polarity'},
                        span({style => 'padding: 0 60px 0 60px'},'20x Polarity'))
                    . $polarity);
    $sadjusted = '';
    $sadjusted = $CLEAR
                 . div({class => 'inputblock',style => 'height: 100%;'},
                       div({class => 'category polarity_adjusted',
                            id => 'polarity_adjusted'},
                           span({style => 'padding: 0 20px 0 20px'},'20x Polarity adjusted'))
                       . $padjusted) if ($padjusted);
  }
  my %options;
  %options = (style => 'background-color: #633') if ($tossed);
  $class .= ' discard' if ($tossed);
  div({class => "line $class",
       id => $line,
       %options},
      div({float => 'left'},$html,
          div({class => 'inputblock',style => 'height: 100%;'},$imagery,$controls)),
      $adjusted,$stable,$sadjusted,$mcfo,
      $CLEAR);
}


sub renderControls
{
  my($ordered,$unordered,$discarded,$export) = @_;
  my $html = div({style => 'float: left; margin-left: 20px;'},
      button(-value => 'Show all lines',
             -class => 'btn btn-success btn-xs',
             -onclick => 'showAll();'),
      button(-value => 'Hide unchecked lines',
             -class => 'btn btn-primary btn-xs',
             -onclick => 'hideUnchecked();'),
      button(-value => 'Hide checked lines',
             -class => 'btn btn-primary btn-xs',
             -onclick => 'hideChecked();') . br . br .
      table({class => 'basic'},
            Tr(td(["Ordered lines ($ordered):",
                   button(-value => 'Show',
                          -class => 'btn btn-success btn-xs',
                          -onclick => 'showByClass("ordered");'),
                   button(-value => 'Hide',
                          -class => 'btn btn-warning btn-xs',
                          -onclick => 'hideByClass("ordered");')])),
            Tr(td(["Unordered lines ($unordered):",
                   button(-value => 'Show',
                          -class => 'btn btn-success btn-xs',
                          -onclick => 'showByClass("unordered");'),
                   button(-value => 'Hide',
                          -class => 'btn btn-warning btn-xs',
                          -onclick => 'hideByClass("unordered");')])),
            Tr(td(["Discarded lines ($discarded):",
                   button(-value => 'Show',
                          -class => 'btn btn-success btn-xs',
                          -onclick => 'showByClass("discard");'),
                   button(-value => 'Hide',
                          -class => 'btn btn-warning btn-xs',
                          -onclick => 'hideByClass("discard");')]))),
           );
  if (scalar keys %MISSING_MIP) {
    $html .= div({class => 'boxed',
                  style => 'float: left; margin-left: 20px;'},
                 span({style => 'color: #f60;font-size: 14pt;'},
                      span({class => 'glyphicon glyphicon-warning-sign'},''),
                      'The following lines are missing imagery'),br,
                 join(br,sort keys %MISSING_MIP));
  }
  $html .= $CLEAR . $export;
}


sub submitButton
{
  my($id,$text) = @_;
  div({align => 'center'},
      submit({&identify($id),
              class => 'btn btn-success',
              value => $text}));
}


sub createAdditionalData
{
  my @other;
  if ($VIEW_ALL) {
    if (param('user')) {
      push @other,Tr(td(['Imaged for: ',&getUsername(param('user'))]));
      push @other,Tr(td(['Image type: ',(param('type') eq 'ti') ? 'Terra incognita' : 'Split screen'])) if param('type');
    }
    elsif ($ACCESS) {
      push @other,Tr(td(['Viewable:',join(br,sort @{$PERMISSION{$USERID}})]));
    }
  }
  unshift @other,Tr(td(['TMOG date range:',"$START - $STOP"]))
    if ($START || $STOP);
  return(@other);
}


sub verifyCrosses
{
  my %line;
  my($total_cross,$total_discard) = (0)x2;
  my ($control,$priority) = ('')x2;
  foreach (param()) {
    $control .= hidden(&identify($_),default => param($_))
      unless ($_ eq 'verify');
    if (/cross$/) {
      $line{join('_',(split('_'))[0,1])} = param($_);
      $total_cross++;
    }
    elsif (/discard$/) {
      $line{join('_',(split('_'))[0,1])} = param($_);
      $total_discard++;
    }
    elsif (/pri$/) {
      $priority = 'Red check marks indicate high-priority crosses.' . br;
    }
  }
  my $normal = '<span class="glyphicon glyphicon-ok" aria-hidden="true"></span>';
  my $high = '<span class="glyphicon glyphicon-ok" style="color: red" aria-hidden="true"></span>';
  my $rbutton = submit({&identify('request'),
                       class => 'btn btn-success',
                       value => 'Request crosses/discards'});
  $rbutton = '' unless ($CAN_ORDER);
  print table({class => 'verify'},
              thead(Tr(th(['Line','Cross barcode',@CROSS,'Discard']))),
              tbody(map {my @col;
                         foreach my $c (@CROSS) {
                           push @col,(param(join('_',$_,lc($c),'cross')))
                                     ? ((param(join('_',$_,lc($c),'pri'))) ? $high : $normal)
                                     : NBSP;
                         }
                         push @col,(param(join('_',$_,'discard'))) ? $normal : NBSP;
                         Tr(th($_),td([$line{$_},@col]));
                        } sort keys %line)),
        br,
        (sprintf 'A total of %d cross%s will be ordered for %s.',
                 $total_cross,(1 == $total_cross) ? '' : 'es',$USERNAME),br,
        $priority,
        (sprintf 'A total of %d line%s will be discarded.',
                 $total_discard,(1 == $total_discard) ? '' : 's'),br,
        div({align => 'center'},
                submit({&identify('cancel'),class => 'btn btn-danger',
                        value => "Cancel",
                        onclick => 'window.location.href="ssplit_review.cgi"'}),
                NBSP,$rbutton),
        $control,
        hidden(&identify('_userid'),default=>param('_userid'));
}


sub requestCrosses
{
  my %type;
  my @row;
  my $head = ['Line','Cross ID','AD line','AD robot ID','DBD line','DBD robot ID',
              'Cross type','Priority','Requester'];
  my (%cross_line,%discard_line);
  my($total_cross,$total_discard) = (0)x2;
  foreach (param()) {
    if (/cross$/) {
      $cross_line{my $l = join('_',(split('_'))[0,1])} = param($_);
      $total_cross++;
      unless ($type{$l}) {
        $sth{DATASET}->execute($l);
        my $ar = $sth{DATASET}->fetchall_arrayref();
        $type{$l} = ($ar->[0][0] =~ /_ti_/) ? 'Terra incognita' : 'Split screen';
      }
    }
    elsif (/discard$/) {
      my $l = join('_',(split('_'))[0,1]);
      $l =~ s/_IS/_SS/;
      $discard_line{$l} = param($_);
      $total_discard++;
    }
  }
  my $json = JSON->new->allow_nonref;
  my $client = REST::Client->new();
  print "Crosses to be ordered: $total_cross" . br;
  my (%error,%success);
  foreach my $line (sort keys %cross_line) {
    $sth{AD_DBD}->execute($line);
    my($ad,$dbd) = $sth{AD_DBD}->fetchrow_array();
    if ($ad && $dbd) {
      $sth{AD_DBD}->execute($line);
      my($ad,$dbd) = $sth{AD_DBD}->fetchrow_array();
      $sth{ROBOT}->execute($ad);
      my($robot_ad) = $sth{ROBOT}->fetchrow_array();
      $sth{ROBOT}->execute($dbd);
      my($robot_dbd) = $sth{ROBOT}->fetchrow_array();
      my @splits = ();
      foreach my $c (@CROSS) {
        next unless (my $barcode = param(join('_',$line,lc($c),'cross')));
        my %split = (ADRobotId => $robot_ad,
                     DBDRobotId => $robot_dbd,
                     lc($c) => 1,
                     priority => (param(join('_',$line,lc($c),'pri'))) ? 2 : 1,
                     line => $line);
        push @splits,\%split;
      }
      my %split = (ADRobotId => $robot_ad,
                   DBDRobotId => $robot_dbd,
                   crossBarcode => $cross_line{$line},
                   line => $line);
      foreach my $c (@CROSS) {
        next unless (my $barcode = param(join('_',$line,lc($c),'cross')));
        $split{lc($c)} = (param(join('_',$line,lc($c),'pri'))) ? 2 : 1;
      }
      @splits = ();
      push @splits,\%split;
      my $order = {username => $USERID,
                   splits => [@splits],
                   specialInstructions => $type{$line},
                   createNewOrder => 0};
      my $json_text = $json->encode($order);
      $client->POST("$FLYSTORE_HOST/api/order/",$json_text);
      if ($client->responseCode() == 201) {
        $success{$line}++;
      }
      else {
        $error{$line} = $json_text . (NBSP)x5 . $client->responseContent();
      }
    }
    else {
      $ad ||= '';
      $dbd ||= '';
      $error{$line} = "Missing AD or DBD (AD: $ad, DBD: $dbd)";
    }
  }
  if (scalar keys %success) {
    print &bootstrapPanel('Lines ordered',join(', ',sort keys %success),'success');
  }
  if (scalar keys %error) {
    print &bootstrapPanel('Could not request the following crosses:',
                          join(br,map {$_ . (NBSP)x5 . $error{$_}} sort keys %error),
                          'danger');
  }
  print &createExportFile(\@row,'_'.$USERID.'_cross_request',$head) if (scalar @row);
  my $count = scalar keys %discard_line;
  if ($count) {
    print hr,"Lines to be discarded: $total_discard" . (br)x2;
    my $order = {username => $USERID,
                 discards => [sort keys %discard_line]};
    my $json_text = $json->encode($order);
    $client->POST("$FLYSTORE_HOST/api/order/",$json_text);
    if ($client->responseCode() == 201) {
      print &bootstrapPanel('Lines discarded',join(', ',sort keys %discard_line),'success');
    }
    else {
      print &bootstrapPanel('Could not discard lines:',
                            join(br,sort keys %discard_line),
                            'danger');
    }
  }
}


sub getUsername
{
  my $userid = shift;
  return($USERNAME{$userid}) if (exists $USERNAME{$userid});
  my $user = $service->getUser($userid);
  $USERNAME{$userid} = join(' ',$user->givenName(),$user->sn());
  return($USERNAME{$userid});
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d_%H%M%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach (@$ar) {
    my @l = @$_;
    print $handle join("\t",@l) . "\n";
  }
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
                  %load);
}
