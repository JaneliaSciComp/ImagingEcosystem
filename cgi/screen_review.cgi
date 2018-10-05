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
use Kafka::Connection;
use Kafka::Producer;
use LWP::Simple qw(get);
use POSIX qw(strftime);
use REST::Client;
use Scalar::Util qw(blessed);
use Time::HiRes qw(gettimeofday tv_interval);
use Time::Local qw(timelocal);
use Try::Tiny;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant NBSP => '&nbsp;';
my $CONFIG_SERVER = 'http://config.int.janelia.org/config';
my (%CONFIG,%SERVER);

# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = '20x screen review';
my $BASE = "/var/www/html/output/";
my $FLYSTORE_HOST = 'http://flystore.int.janelia.org';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my @CROSS = qw(Polarity MCFO Stabilization);
my $STACK = 'view_sage_imagery.cgi?_op=stack;_family=split_screen_review;_image';
my $PRIMARY_MIP = 'Signal 1 MIP';
my $SECONDARY_MIP = 'Signal MIP ch1';
my @UNUSABLE = qw(Dead Exit Tossed);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
our ($dbh,$dbhf);
my %sth = (
CANORDER => "SELECT data_set FROM image_data_mv WHERE data_set LIKE ? LIMIT 1",
DATASET => "SELECT DISTINCT value FROM image_vw i JOIN image_property_vw ip "
           . "ON (i.id=ip.image_id AND ip.type='data_set') WHERE i.line=? "
           . "AND i.family LIKE '%screen_review'",
HALVES => "SELECT lp.value,lp.name,lpp.value AS info FROM line_property_vw lp "
          . "JOIN line_relationship_vw lr ON (lr.object=lp.name AND "
          . "lr.relationship='child_of') JOIN line_property_vw lpp ON "
          . "(lp.name=lpp.name AND lpp.type='flycore_production_info') WHERE "
          . "lp.type='flycore_project' AND lr.subject=?",
IMAGESL => "SELECT i.name FROM image_vw i LEFT OUTER JOIN image_property_vw ipd "
          . "ON (i.id=ipd.image_id AND ipd.type='data_set') WHERE i.line=?",
IMAGES => "SELECT line,i.name,data_set,slide_code,area,cross_barcode,lpr.value AS requester,channel_spec,lsm_illumination_channel_1_power_bc_1,lsm_illumination_channel_2_power_bc_1,lsm_detection_channel_1_detector_gain,lsm_detection_channel_2_detector_gain,im.url,la.value,DATE(i.create_date) FROM image_data_mv i JOIN image im ON (im.id=i.id) LEFT OUTER JOIN line_property_vw lpr ON (i.line=lpr.name AND lpr.type='flycore_requester') JOIN line l ON (i.line=l.name) LEFT OUTER JOIN line_annotation la ON (l.id=la.line_id) WHERE data_set LIKE ? AND line LIKE 'LINESEARCH' AND i.display!=0 ORDER BY 1",
SIMAGES => "SELECT i.name,area,im.url,lsm_illumination_channel_1_power_bc_1,lsm_illumination_channel_2_power_bc_1,lsm_detection_channel_1_detector_gain,lsm_detection_channel_2_detector_gain,channel_spec,data_set,objective,DATE(i.create_date),slide_code,lpr.value AS requester FROM image_data_mv i JOIN image im ON (im.id=i.id) LEFT OUTER JOIN line_property_vw lpr ON (i.line=lpr.name AND lpr.type='flycore_requester') WHERE line=? AND data_set LIKE ? ORDER BY slide_code,area",
SSCROSS => "SELECT line,cross_type FROM cross_event_vw WHERE line LIKE "
           . "'JRC\_SS%' AND cross_type IN ('SplitFlipOuts','SplitPolarity','StableSplitScreen') GROUP BY 1,2",
ROBOT => "SELECT robot_id FROM line_vw WHERE name=?",
USERLINES => "SELECT SUBSTRING_INDEX(value,'_',1),COUNT(DISTINCT line) FROM image_vw i "
             . "JOIN image_property_vw ip ON (i.id=ip.image_id AND ip.type='data_set') "
             . "WHERE value LIKE '%screen_review' GROUP BY 1",
# ----------------------------------------------------------------------------
FB_ONROBOT => "SELECT Stock_Name,Production_Info,On_Robot,GROUP_CONCAT("
              . "DISTINCT lab_member) FROM StockFinder sf LEFT OUTER JOIN "
              . "Project_Crosses pc ON (sf.__kp_UniqueID=pc._kf_Parent_UID) "
              . "WHERE Stock_Name LIKE 'JRC\_SS%' GROUP BY 1,2,3",
);
my $CLEAR = div({style=>'clear:both;'},NBSP);
my (%BRIGHTNESS,%DISCARD,%GAIN,%ONORDER,%PERMISSION,%POWER,%REQUESTER,%SSCROSS,%USERNAME);
my (%DATA_SET,%MISSING_HALF,%MISSING_MIP,%STABLE_SHOWN);
my @performance;
my $ACCESS = 0;
my $split_name = '';
my $IMAGE_ID = 1;
# Kafka
my ($connection,$producer,$kafka_msg);

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
my $RUN_AS = ($VIEW_ALL && param('_userid')) ? param('_userid') : '';
my $CAN_ORDER = ($VIEW_ALL) ? 0 : 1;
$CAN_ORDER = 0 if ($USERID eq 'dolanm' || $RUN_AS eq 'dolanm');
$CAN_ORDER = 1 if ($USERID eq 'svirskasr' && $RUN_AS);
$CAN_ORDER = 1 if ($USERID eq 'dicksonb' || $USERID eq 'rubing');
my $START = param('start') || '';
my $STOP = param('stop') || '';
my $ALL_20X = (param('all_20x') && (param('all_20x') eq 'all'));
# Initialize
&initializeProgram();

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
elsif (param('sline')) {
  &showLine(param('sline'));
}
elsif (($VIEW_ALL && !$RUN_AS && !param('user')) || $ACCESS) {
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
}
exit(0);

# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub getREST
{
  my($server,$endpoint) = @_;
  my $url = join('/',$CONFIG{$server}{url},$endpoint);
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
  # Get servers
  my $rest = $CONFIG_SERVER . '/servers';
  my $response = get $rest;
  my $rvar;
  eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
  %SERVER = %{$rvar->{config}};
  # Get WS REST config
  $rest = $CONFIG_SERVER . '/rest_services';
  $response = get $rest;
  eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
  %CONFIG = %{$rvar->{config}};
  # Modify primary search statement
  if (my $l = param('line')) {
    $l = '%' . uc($l) . '%';
    $sth{IMAGES} =~ s/ LIKE 'LINESEARCH'/ LIKE '$l'/;
    $START = $STOP = '';
  }
  elsif (my $sc = param('slide_code')) {
    $sc = uc($sc) . '%';
    $sth{IMAGES} =~ s/line LIKE 'LINESEARCH'/slide_code LIKE '$sc'/;
    $START = $STOP = '';
  }
  else {
    if ((param('search_mode')) && (param('search_mode') eq 'ss')) {
      $sth{IMAGES} =~ s/ LIKE 'LINESEARCH'/ LIKE '%\_SS%'/;
    }
    else {
      $sth{IMAGES} =~ s/ LIKE 'LINESEARCH'/ LIKE '%\_IS%'/;
    }
    if ($START && $STOP) {
      $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) BETWEEN '$START' AND '$STOP' AND /;
    }
    elsif ($START) {
      $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) >= '$START' AND /;
    }
    elsif ($STOP) {
      $sth{IMAGES} =~ s/WHERE /WHERE DATE(i.create_date) <= '$STOP' AND /;
    }
  }
  print STDERR "Primary query: $sth{IMAGES}\n";
  # Get user permissions
  my $CUSER = $RUN_AS || $USERID;
  $rest = $CONFIG_SERVER . "/$PROGRAM";
  $response = get $rest;
  eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
  %PERMISSION = %{$rvar->{config}};
  ($ACCESS,$CAN_ORDER,$VIEW_ALL) = (1,1,1)
    if (exists $PERMISSION{$CUSER});
  # Change permission query
  if ($ACCESS) {
    my $stmt = "IN ('" . join("','",@{$PERMISSION{$CUSER}}) . "')";
    $sth{USERLINES} =~ s/LIKE '%screen_review'/$stmt/;
    print STDERR "User query: $sth{USERLINES}\n";
  }
  # Connect to databases
  &dbConnect(\$dbh,'sage')
    || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
  &dbConnect(\$dbhf,'flyboy')
    || &terminateProgram("Could not connect to FlyBoy: ".$DBI::errstr);
  foreach (keys %sth) {
    if (/^FB_/) {
      (my $n = $_) =~ s/FB_//;
      $sth{$n} = $dbhf->prepare($sth{$_}) || &terminateProgram($dbhf->errstr);
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
    }
  }
  # Can stocks be ordered?
  $sth{CANORDER}->execute($CUSER . '%_screen_review');
  my $ar = $sth{CANORDER}->fetchall_arrayref();
  $CAN_ORDER = 1 if (scalar @$ar);
  # Kafka
    try {
    $connection = Kafka::Connection->new(host => $SERVER{Kafka}{address},
                                         timeout => 3);
    $producer = Kafka::Producer->new(Connection => $connection)
      if ($connection);
  }
  catch {
    my $error = $_;
    if (blessed($error) && $error->isa('Kafka::Exception')) {
      print STDERR 'Error: (' . $error->code . ') ' . $error->message . "\n";
    }
    else {
      print STDERR "$error\n";
    }
  };
  push @performance,sprintf 'Initialization: %.4f sec',tv_interval($t0,[gettimeofday]);
}


sub showUserDialog()
{
  print div({class => 'boxed'},
            &dateDialog(),
            &submitButton('choose','Search')),br,
           hidden(&identify('_userid'),default=>param('_userid'));
}


sub showLine
{
  my $line = shift;
  my @image;
  $sth{IMAGESL}->execute($line);
  my $ar = $sth{IMAGESL}->fetchall_arrayref();
  foreach (@$ar) {
    (my $wname = $_->[0]) =~ s/.+\///;
    $wname =~ s/\.bz2//;
    $wname .= '.bz2';
    my($signalmip,undef) = &getSingleMIP($wname);
    push @image,img({src => $SERVER{'jacs-storage'}{address} . $signalmip});
  }
  print div({align => 'center',
             style => 'padding: 20px 0 20px 0; background-color: #111;'},@image);
}


sub getSingleMIP
{
  my($wname) = shift;
  my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{LSMImages} . "?name=$wname";
  my $response = get $rest;
  return('','') unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  (my $sample = $rvar->{sample} || '') =~ s/Sample#//;
  return($rvar->{files}{$PRIMARY_MIP}||$rvar->{files}{$SECONDARY_MIP}||'',$rvar->{brightnessCompensation}||0,$sample);
}


sub limitFullSearch
{
  $sth{USERLINES}->execute();
  my $ar = $sth{USERLINES}->fetchall_arrayref();
  my %label = map {$a = (split('_',$_->[0]))[0];
                   $a => &getUsername($a) . " ($_->[1] lines)"} @$ar;
  $label{''} = '(Any)';
  my %screen_count = (split => 0, ti => 0, is => 0, ss => 0);
  $screen_count{$_->[0]} += $_->[1] foreach (@$ar);
  #my $type = {'' => '(Any)',
  #            split => "Split screen ($screen_count{split} lines)",
  #            ti => "Terra incognita ($screen_count{ti} lines)"};
  print div({class => 'boxed'},
            table({class => 'basic'},
                  Tr(td('User:'),
                     td(popup_menu(&identify('user'),
                                   -values => [sort keys %label],
                                   -labels => \%label))),
                  #Tr(td('Image type:'),
                  #   td(popup_menu(&identify('type'),
                  #                 -values => ['','split','ti'],
                  #                 -labels => $type)))
                 ),
            &dateDialog(),
            hidden(&identify('_userid'),default=>param('_userid')),
            &submitButton('choose','Search')),br;
}


sub dateDialog
{
  my $ago = strftime "%F",localtime (timelocal(0,0,12,(localtime)[3,4,5])-(60*60*24*30));
  my $margin = 'border-right: 1px solid #aaa; padding-right: 50px; margin-right: 50px;';
  my $datesect = div({style => $margin . 'float: left'},
                     table({class => 'basic'},
                           Tr(td('Start TMOG date:'),
                              td(input({&identify('start'),
                                        value => $ago}) . ' (optional)')),
                           Tr(td('Stop TMOG date:'),
                              td(input({&identify('stop')}) . ' (optional)'))),
                     'Search by: ',
                     radio_group(&identify('search_mode'),
                                 -values => ['is','ss'],
                                 -labels => {is => ' Initial split',
                                             ss => ' Stable split'},
                                 -default => 'is'));
  my $lsect = div({style => $margin . 'float: left'},'Line: ' . input({&identify('line')}) . ' (optional)' . br
                  'Enter any portion of an Initial or Stable' . br . 'split line name');
  my $scsect = div({style => 'float: left;'},'Slide code: ' . input({&identify('slide_code')}) . ' (optional)'
                   . br . 'Enter the start (date or date+slide#) of any slide code');
  my($all,$mcfo) = ('')x2;
  if ($VIEW_ALL) {
    $mcfo = ('Display MCFO imagery: ' . input({&identify('show_mcfo'),type => 'checkbox'}) . br);
    $all = '20X stable imagery to display: ' .
           radio_group(&identify('all_20x'),
                       -values => ['first','all'],
                       -labels => {first => ' First 20X stable split only',
                                   all => ' All 20X stable splits'},
                       -default => 'first') . br;
  }
  (div({class => 'boxed', style => 'float: left'},$datesect,$lsect,$scsect),$CLEAR,
   $mcfo,$all,
   'Display imagery as grayscale: ',input({&identify('grayscale'),type => 'checkbox'}),
  );
}


sub chooseCrosses
{
  my %lines = ();
  my ($adjusted,$class,$controls,$lhtml,$imagery,$last_line,$mcfo,$sss,
      $sss_adjusted,$polarity,$polarity_adjusted,$halferr) = ('')x12;
  my $DSUSER = $USERID;
  my $DSTYPE = '%';
  if ($VIEW_ALL) {
    $DSUSER = param('user') || $RUN_AS || '%';
    #$DSTYPE = param('type') if (param('type'));
  }
  my $ds = $DSUSER . '\_' . $DSTYPE . '\_screen\_review';
  $t0 = [gettimeofday];
  print STDERR "Primary query parm: $ds\n";
  $sth{IMAGES}->execute($ds);
  my $ar = $sth{IMAGES}->fetchall_arrayref();
  if ($ACCESS) {
    my @arr = @$ar;
    @$ar = ();
    my @list = @{$PERMISSION{$RUN_AS || $USERID}};
    foreach my $l (@arr) {
      next unless (grep(/$l->[2]/,@list));
      push @$ar,$l;
    }
  }
  push @performance,sprintf 'Primary query: %.4f sec (rows: %d)',
                            tv_interval($t0,[gettimeofday]),scalar(@$ar);
  unless (scalar @$ar) {
    print &bootstrapPanel('No screen imagery found',
                          'No screen imagery was found'
                          . (($DSUSER eq '%') ? '.' : " for $DSUSER")
                          . br . "Search term: [$ds]",'danger');
    return;
  }
  my $html = '';
  my($discarded,$ordered,$tossed,$with_ss) = (0)x4;
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
    next if (exists $STABLE_SHOWN{$l->[0]});
    # Line, image name, data set, slide code, area, cross barcode, requester,
    # channel spec, power 1, power 2, gain 1, gain 2, url, comment, TMOG date
    my($line,$name,$dataset,$slide,$area,$barcode,$requester,
       $chanspec,$power1,$power2,$gain1,$gain2,$url,$comment,$tmog_date) = @$l;
    my($power,$gain) = ($power{$line}{$area},$gain{$line}{$area});
    $lines{$line}++;
    $DATA_SET{$line} = $dataset;
    my $hover_requester = $requester || &getUsername((split('_',$dataset))[0]);
    # Line control break
    if ($line ne $last_line) {
      $html .= &renderLine($last_line,$lhtml,$imagery,$adjusted,$mcfo,
                           $sss,$sss_adjusted,$polarity,$polarity_adjusted,
                           $controls,$class,$tossed) if ($lhtml);
      ($lhtml,$halferr) = &createLineHeader($line,$dataset,$barcode,$requester,$slide,$comment);
      $last_line = $line;
      ($class,$imagery,$adjusted,$tossed) = ('unordered','','',0);
      %crossed = ();
      my %cross_type = ();
      if ($barcode && (! exists $MISSING_HALF{$line})) {
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
          if ($DISCARD{$stable_line}) {
            $discarded++;
            $tossed = 1;
          }
          my $available;
          if (exists $REQUESTER{$stable_line}) {
            if ($VIEW_ALL) {
              $available = "(available: ordered by $REQUESTER{$stable_line})";
            }
            else {
              $available = (index($REQUESTER{$stable_line},$USERNAME) >= 0)
                           ? '(available: ordered by you)'
                           : '(available: ordered by others, contact Fly Facility)';
            }
          }
          else {
            $available = '(available: unknown requester, contact Fly Facility)';
          }
          my $link = a({href => "lineman.cgi?line=$stable_line",
                        target => '_blank'},
                       (($tossed) ? $stable_line : $available),'');
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
        if ($halferr && $halferr eq 'ls') {
          my $msg = ($halferr eq 'ls')
            ? 'AD/DBD landing sites match - contact the Fly Facility'
            : 'One or more '
              . span({style => 'background:#fc9'},'split halves')
              . ' is unavailable';
          $controls = &bootstrapPanel('Cannot order stable splits',
                          span({style => 'color:#000'},$msg));
        }
        elsif ($tossed) {
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
        ($mcfo) = &getStableImagery($stable_line,$DSUSER.'\_mcfo%') if (param('show_mcfo'));
        ($sss,$sss_adjusted) = &getStableImagery($stable_line,$DSUSER.'\_%\_screen\_review');
        if ($sss) {
          $ordered-- if ($bump_ordered);
          $class = 'stableimg';
          $with_ss++;
        }
        else {
          ($polarity,$polarity_adjusted) = &getStableImagery($stable_line,$DSUSER.'\_polarity%');
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
    $imagery .= &addSingleImage($line,$name,$area,$url,$power,$gain,'',$tmog_date,&getHover($slide,$hover_requester));
    $adjusted .= &addSingleImage($line,$name,$area,$url,$power,$gain,'',$tmog_date,'',1)
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

  push @performance,sprintf 'Main loop: %.4f sec',
                    (my $elapsed_time = tv_interval($t0,[gettimeofday]));
  my $export_button = '';
  if (scalar @export) {
    push @export,['TOTAL',@cross_count{'Line',@CROSS}];
    $export_button = &createExportFile(\@export,'_'.$USERID.'_screen_review',
                                       ['Annotator','Line',@CROSS]);
  }
  my $uname = $USERNAME;
  $uname .= " (running as $RUN_AS)" if ($RUN_AS);
  my @other = &createAdditionalData();
  $kafka_msg->{elapsed_time} = $elapsed_time;
  &publish(encode_json($kafka_msg));
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
                            $with_ss,$export_button),
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
    next if ((!exists $POWER{$line}{Brain}) || (!exists $POWER{$line}{VNC}));
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
  my($line,$dataset,$barcode,$requester,$slide,$comment) = @_;
  my($split_halves,$error) = &getSplitHalves($line,1);
  my $type = ($dataset =~ /_ti_/) ? 'Terra incognita' : 'Split screen';
  my $lhtml = h3(&lineLink($line) . (NBSP)x5 . $type);
  my $bh = ($barcode) ? a({href => "/flyboy_search.php?cross=$barcode",
                           target => '_blank'},$barcode) : '';
  my @row = Tr(td(['Cross barcode:',$bh]));
  push @row,Tr(td(['Data set:',$dataset])) if ($VIEW_ALL);
  $requester = &getUsername((split('_',$dataset))[0])
    if ($VIEW_ALL && !param('user') && !$requester);
  push @row,Tr(td(['Requester:',$requester])) if ($requester);
  push @row,Tr(td(['Slide code:',$slide])) if ($slide);
  $comment ||= '';
  push @row,Tr(td(['Comment:',
                   ($CAN_ORDER) ? div({&identify($line.'_comment'),
                                       class => 'edit'},$comment)
                                : $comment]));
  $lhtml .= table({class => 'basic'},@row);
  $lhtml .= $split_halves if ($split_halves);
  return($lhtml,$error);
}


sub getFlyStoreOrders
{
  my $ar = shift;
  my (%is_lines,%discards);
  my($is_lines,$discards);
  my $client = REST::Client->new();
  my $json = JSON->new->allow_nonref;
  foreach (@$ar) {
    $is_lines{$_->[0]}++;
    ($a = $_->[0]) =~ s/IS/SS/;
    $discards{$a}++;
  }
  @$is_lines = keys %is_lines;
  @$discards = keys %discards;
  my $post_hash = {is_lines => $is_lines,discards => $discards};
  my $count = scalar(@$is_lines) + scalar(@$discards);
  $client->POST("$FLYSTORE_HOST/api/orders/batch/",$json->encode($post_hash));
  if ($client->responseCode() != 200) {
    print &bootstrapPanel("FlyStore could not process IS/discard check "
                          . "on $count lines",,
                          Dumper($post_hash),'danger');
    &terminateProgram('Error response (' . $client->responseCode()
                      . ') from FlyStore for IS/discard check: '
                      . $client->responseContent());
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
  foreach (@$ar2) {
    $a = $_->[1] || '';
    $DISCARD{$_->[0]}++ if ($a && grep(/$a/,@UNUSABLE));
    $_->[2] ||= '';
    $REQUESTER{$_->[0]} = $_->[3] if ($_->[2] eq 'Yes');
  }
  foreach my $order (keys %{$struct->{discards}}) {
    foreach my $l (@{$struct->{discards}{$order}{stockName}}) {
      $DISCARD{$l}++;
    }
  }
  push @performance,sprintf 'Discard hash build for %d stocks: %.4f sec',$count,tv_interval($t0,[gettimeofday]);
  # Stable stocks on SAGE
  $t0 = [gettimeofday];
  $sth{SSCROSS}->execute();
  $ar = $sth{SSCROSS}->fetchall_arrayref();
  $SSCROSS{$_->[0]}{$_->[1]}++ foreach (@$ar);
  push @performance,sprintf 'Stable split cross hash build: %.4f sec',tv_interval($t0,[gettimeofday]);
}


sub getHover
{
    my($slide_code,$requester) = @_;
    my @hover;
    push @hover,"Slide code: $slide_code" if ($slide_code);
    push @hover,"Requester: $requester" if ($requester);
    return(scalar(@hover) ? join("\n",@hover) : '');
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
    next unless ($i->[-4] =~ /20[Xx]/);
    last if ((!$ALL_20X) && ($used >= 2));
    my($power,$gain) = &getPowerGain($i->[-6],@{$i}[3..6]);
    $POWER{$line}{$i->[1]} = $power;
    $GAIN{$line}{$i->[1]} = $gain;
    $used++;
  }
  &populateBrightness($line,$ar,1);
  $used = 0;
  foreach my $i (@$ar) {
    my $requester = pop @$i;
    my $slide_code = pop @$i;
    my $tmog_date = pop @$i;
    my $objective = pop @$i;
    next unless ($objective =~ /20[Xx]/);
    $STABLE_SHOWN{$line}++;
    last if ((!$ALL_20X) && ($used >= 2));
    # Image name, area, url, power 1, power 2, gain 1, gain 2, channel spec, data set
    splice(@$i,3,6,$POWER{$line}{$i->[1]},$GAIN{$line}{$i->[1]},$i->[-1]);
    push @$i,$tmog_date;
    my $hover_requester = $requester || &getUsername((split('_',$i->[-2]))[0]);
    $img .= &addSingleImage($line,@$i,&getHover($slide_code,$hover_requester));
    $adjusted .= &addSingleImage($line,@$i,'',1)
      if (exists $BRIGHTNESS{$line}{$i->[1]});
    $used++;
  }
  return($img,$adjusted);
}


sub getSplitHalves
{
  my($line,$return_html) = @_;
  $sth{HALVES}->execute($line);
  my $hr = $sth{HALVES}->fetchall_hashref('value');
  my($html,$split_name) = ('')x2;
  my %hash;
  $MISSING_HALF{$line}++ unless (scalar(keys %$hr) > 1);
  my $error = 0;
  if (scalar(keys %$hr)) {
    $html = join(br,table({class => 'halves'},
                          map {Tr(th($_.':'),td(&lineLink($hr->{$_}{name},
                                                          $hr->{$_}{info})))}
                              sort keys %$hr));
    $split_name = join('-x-',map {$hr->{$_}{name}} sort keys %$hr);
    my($ad,$dbd) = map {$hr->{$_}{name}} sort keys %$hr;
    foreach (keys %$hr) {
      $hash{(/AD$/) ? 'ad' : 'dbd'} = {name => $hr->{$_}{name},
                                       info => $hr->{$_}{info}};
      $a = $hr->{$_}{info};
      $error = 'unavailable' if ($a && grep(/$a/,@UNUSABLE));
    }
    if (scalar(keys %$hr) == 2) {
      $error = 'ls'
        if (substr($ad,-2) eq substr($dbd,-2));
    }
  }
  ($return_html) ? return($html,$error) : return(\%hash);
}


sub lineLink
{
  my($l,$info) = @_;
  $info ||= '';
  my($shown,$unusable) = ($l,0);
  if ($info && grep(/$info/,@UNUSABLE)) {
    $unusable++;
    $shown .= " ($info)" 
  }
  my $link = a({href => 'lineman.cgi?line='.$l,
                target => '_blank'},$shown);
  return(($unusable) ? span({style => 'background:#fc9'},$link)
                     : $link);
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
  my($line,$name,$area,$url,$power,$gain,$dataset,$tmog_date,$hover,$adjusted) = @_;
  $dataset ||= '';
  (my $wname = $name) =~ s/.+\///;
  $wname =~ s/\.bz2//;
  my($bc,$signalmip,$sample) = ('')x3;
  ($signalmip,$bc,$sample) = &getSingleMIP($wname);
  ($signalmip,$bc,$sample) = &getSingleMIP($wname.'.bz2') unless ($signalmip);
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
    $i = $SERVER{'jacs-storage'}{address} . "/$i";
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
    my $url2 = $SERVER{'jacs-storage'}{address} . "/$signalmip";
    my $caption=$line;
    $caption .= " ($split_name)" if ($split_name);
    $signalmip = a({href => "view_image.cgi?url=$url2"
                            . "&caption=$caption" . $parms,
                    target => '_blank'},
                   img({id => ('imgt' . $IMAGE_ID++),
                        class => 'ti',
                        style => $style,
                        title => $hover,
                        src => $url2, height => $HEIGHT}));
  }
  (my $all = $signal) =~ s/signal.+mp4$/all.mp4/;
  $signal = a({href => $SERVER{'jacs-storage'}{address} . "/$signal",
               target => '_blank'},
              img({src => '/images/stack_plain.png',
                   title => 'Show signal movie'}));
  $all = a({href => $SERVER{'jacs-storage'}{address} . "/$all",
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
    $PREFIX =~ s/(?:split|ti|is|ss)_screen_review/flylight_flip/;
  }
  elsif ($dataset =~ /polarity/) {
    $PREFIX =~ s/(?:split|ti|is|ss)_screen_review/flylight_polarity/;
  }
  my @row = ();
  my %opt = (class => 'imgoptions');
  ($url,$signal,$all,$opt{class}) = ('')x4 if ($adjusted);
  push @row,Tr(td({colspan => 5},$pgv));
  my $link = ($sample) ? "http://webstation.int.janelia.org/do/Sample:$sample"
                       : "$PREFIX=$name";
  div({class => 'single_mip'},$signalmip,br,
      table(Tr(td({width => '14%'},$url),
               td({width => '14%'},NBSP),
               td({width => '44%'},a({href => $link,
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
  $imagery = $adjusted = '' if ($line =~ /SS/);
  $adjusted = $CLEAR
              . div({class => 'inputblock',style => 'height: 100%;'},
                    div({class => 'category initialsplit_adjusted'},
                        span({style => 'padding: 0 60px 0 60px'},'Adjusted'))
                    . $adjusted) if ($adjusted);
  $mcfo = $CLEAR
          . div({class => 'inputblock',style => 'height: 100%;'},
                div({class => 'category mcfo'},
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
      div({style => 'float: left'},
          div({style => 'float: left;min-width: 475px;'},$html),
          div({style => 'float: left;'},(br)x2,$controls),
          $CLEAR,
          div({class => 'inputblock',style => 'height: 100%;'},$imagery)),
      $adjusted,$stable,$sadjusted,$mcfo,
      $CLEAR);
}


sub renderControls
{
  my($ordered,$unordered,$discarded,$with_ss,$export) = @_;
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
                          -onclick => 'hideByClass("discard");')])),
            Tr(td(["Lines with 20x stable split imagery ($with_ss):",
                   button(-value => 'Show',
                          -class => 'btn btn-success btn-xs',
                          -onclick => 'showByClass("stableimg");'),
                   button(-value => 'Hide',
                          -class => 'btn btn-warning btn-xs',
                          -onclick => 'hideByClass("stableimg");')]))),
           );
  if (scalar keys %MISSING_MIP) {
    $html .= div({class => 'boxed',
                  style => 'float: left; margin-left: 20px;'},
                 span({style => 'color: #f60;font-size: 14pt;'},
                      span({class => 'glyphicon glyphicon-warning-sign'},''),
                      'The following lines are missing imagery'),br,
                 join(', ',map {a({href => "/cgi-bin/lineman.cgi?line=$_",
                                   target => '_blank'},$_)}
                               sort keys %MISSING_MIP));
  }
  my $thumb = table({class => 'basic'},
                    Tr(td('Thumbnail size: '),
                       td(div({style => 'background-color:#eeeeee'},
                          input({id => 'sSlider',
                              type => 'range',
                              min => 0,
                              max => 400,
                              step => 5,
                              value => 0,
                              onchange => "changeSlider('s');"}) .
                       span({id => 's'},'100%')))));
  my $warn = '';
  if ($CAN_ORDER) {
    $warn = div({class => 'boxed',style => 'border-color: #6c0'},
                span({style => 'color: #6c0;font-size: 14pt;'},
                     span({class => 'glyphicon glyphicon-ok'},''),
                        'You are authorized to order crosses'));
  }
  else {
    $warn = div({class => 'boxed',style => 'border-color: #f00'},
                span({style => 'color: #f00;font-size: 14pt;'},
                     span({class => 'glyphicon glyphicon-remove'},''),
                        'You are not authorized to order crosses'));
  }
  $html .= div({style=>'clear:both;',class=>'left'},
               div({class => 'left'},$export),
               div({class => 'left'},$thumb),
               div({class => 'left'},$warn)
              );
  $html .= div({style=>'clear:both;'},NBSP);
  return($html);
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
  $kafka_msg = {program => $PROGRAM,
                user => $USERID,
                operation => 'search'};
  if ($VIEW_ALL) {
    if (param('user')) {
      push @other,Tr(td(['Imaged for: ',&getUsername(param('user'))]));
      #push @other,Tr(td(['Image type: ',(param('type') eq 'ti') ? 'Terra incognita' : 'Split screen'])) if param('type');
    }
    elsif ($ACCESS) {
      push @other,Tr(td(['Viewable:',join(br,sort @{$PERMISSION{$RUN_AS || $USERID}})]));
    }
  }
  if (param('line')) {
    unshift @other,Tr(td(['Line search term:',param('line')]));
    $kafka_msg->{line} = param('line');
  }
  elsif (param('slide_code')) {
    unshift @other,Tr(td(['Slide code search term:',param('slide_code')]));
    $kafka_msg->{slide_code} = param('slide_code');
  }
  else {
    unshift @other,Tr(td(['TMOG date range:',"$START - $STOP"]));
    unshift @other,Tr(td(['Search mode',(param('search_mode') eq 'is') ? 'Initial' : 'Stable']));
    $kafka_msg->{date_range} = "$START - $STOP";
    $kafka_msg->{search_mode} = param('search_mode');
  }
  return(@other);
}


sub publish
{
  return unless ($producer);
  my($message) = shift;
  try {
    my $t = time;
    my $stamp = strftime "%Y-%m-%d %H:%M:%S", localtime $t;
    $stamp .= sprintf ".%03d", ($t-int($t))*1000;
    my $response = $producer->send('split_screen',0,$message,$stamp,undef,time*1000);
  }
  catch {
    my $error = $_;
    if (blessed($error) && $error->isa('Kafka::Exception')) {
      print STDERR 'Error: (' . $error->code . ') ' . $error->message . "\n";
    }
    else {
      print STDERR "$error\n";
    }
  };
}


sub verifyCrosses
{
  my (%error,%line);
  my($total_cross,$total_discard) = (0)x2;
  my ($control,$priority,$unusable,$warn) = ('')x4;
  my($ad,$dbd) = ('-')x2;
  foreach (param()) {
    next if (/^(?:_|verify)/);
    $control .= hidden(&identify($_),default => param($_))
      unless ($_ eq 'verify');
    my $l = join('_',(split('_'))[0,1]);
    if (/cross$/) {
      $line{$l}{p} = param($_);
      my $resp = &getREST('sage',"split_order/$l");
      unless ($resp) {
        $error{$l}{p} = param($_);
        delete $line{$l};
        next;
      }
      $total_cross++;
      foreach my $sh ('ad','dbd') {
        if (exists $resp->{split_halves}{original}) {
          $line{$l}{$sh} = span({style => 'color:#f60'},
                                $resp->{split_halves}{order}{$sh});
          $unusable++;
        }
        else {
          $line{$l}{$sh} = $resp->{split_halves}{order}{$sh};
        }
      }
    }
    elsif (/discard$/) {
      $line{$l}{p} = param($_);
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
  $warn = div({class => 'boxed',style => 'border-color: #f60'},
              span({style => 'color: #f60;font-size: 14pt;'},
                   span({class => 'glyphicon glyphicon-warning-sign'},''),
                        'Split halves in orange have been substituted for unavailable split halves'))
    if ($unusable);
  $warn .= div({class => 'boxed',style => 'border-color: #f00'},
               span({style => 'color: #f00;font-size: 14pt;'},
                    span({class => 'glyphicon glyphicon-remove'},''),
                         'The following lines cannot have splits ordered: '
                         . join(', ',sort(keys %error))))
    if (scalar(keys %error));
  print table({class => 'verify'},
              thead(Tr(th(['Line','Cross barcode',@CROSS,'Discard','AD','DBD']))),
              tbody(map {my @col;
                         foreach my $c (@CROSS) {
                           push @col,(param(join('_',$_,lc($c),'cross')))
                                     ? ((param(join('_',$_,lc($c),'pri'))) ? $high : $normal)
                                     : NBSP;
                         }
                         push @col,(param(join('_',$_,'discard'))) ? $normal : NBSP;
                         Tr(th($_),td([$line{$_}{p},@col,
                                       $line{$_}{ad},$line{$_}{dbd}]));
                        } sort keys %line)),
        br,$warn,
        (sprintf 'A total of %d cross%s can be ordered for %s.',
                 $total_cross,(1 == $total_cross) ? '' : 'es',$USERNAME),br,
        $priority,
        (sprintf 'A total of %d line%s can be discarded.',
                 $total_discard,(1 == $total_discard) ? '' : 's'),br,
        (($CAN_ORDER) ? 'Press the "Request crosses/discards" button to place your order.'
                      : "This is simply a verification screen; no order will be placed."),
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
  my (%diagnostic,%error,%success);
  foreach my $line (sort keys %cross_line) {
    my($ad,$dbd,$robot_ad,$robot_dbd);
    my $resp = &getREST('sage',"split_order/$line");
    unless ($resp) {
      $error{$line} = 'One or more split halves is unavailable';
      next;
    }
    $ad = $resp->{split_halves}{order}{ad};
    $dbd = $resp->{split_halves}{order}{dbd};
    $robot_ad = $resp->{split_halves}{order}{ad_robot_id};
    $robot_dbd = $resp->{split_halves}{order}{dbd_robot_id};
    my %split = (ADRobotId => $robot_ad,
                 DBDRobotId => $robot_dbd,
                 crossBarcode => $cross_line{$line},
                 line => $line);
    foreach my $c (@CROSS) {
      next unless (my $barcode = param(join('_',$line,lc($c),'cross')));
      $split{lc($c)} = (param(join('_',$line,lc($c),'pri'))) ? 2 : 1;
    }
    my $order = {username => $USERID,
                 splits => [\%split],
                 specialInstructions => $type{$line},
                 createNewOrder => 0};
    my $json_text = $json->encode($order);
    if ($RUN_AS) {
      $diagnostic{$line} = $json_text;
    }
    else {
      $client->POST("$FLYSTORE_HOST/api/order/",$json_text);
      $kafka_msg = {program => $PROGRAM,
                    user => $USERID,
                    operation => 'order',
                    order => $order};
      &publish(encode_json($kafka_msg));
      if ($client->responseCode() == 201) {
        $success{$line}++;
      }
      else {
        $error{$line} = $json_text . (NBSP)x5 . $client->responseContent();
      }
    }
  }
  if (scalar keys %success) {
    print &bootstrapPanel('Lines ordered',join(', ',sort keys %success),'success');
  }
  if (scalar keys %diagnostic) {
    print &bootstrapPanel('Would have requested the following crosses:',
                          join(br,map {$_ . (NBSP)x5 . $diagnostic{$_}} sort keys %diagnostic),
                          'warning');
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
  # Set up LDAP service
  my $service = JFRC::LDAP->new({host => 'ldap-vip3.int.janelia.org'});
  my $user = $service->getUser($userid);
  $USERNAME{$userid} = ($user) ? join(' ',$user->givenName(),$user->sn()) : $userid;
  return($USERNAME{$userid});
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d_%H%M%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach my $r (@$ar) {
    my @l = @$r;
    foreach (2..4) {
      $l[$_] ||= '';
    }
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
