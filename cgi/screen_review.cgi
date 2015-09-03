#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use CGI::Carp qw(fatalsToBrowser);
use DBI;
use IO::File;
use JFRC::LDAP;
use JSON;
use POSIX qw(strftime);
use REST::Client;
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Slime qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/opt/informatics/data/';
use constant NBSP => '&nbsp;';

# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = '20x screen review';
my $BASE = "/var/www/html/output/";
my $FLYSTORE_HOST = 'flystore.int.janelia.org';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my @CROSS = qw(Polarity MCFO Stabilization);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
my %sth = (
AD_DBD => "SELECT MAX(ad.name),MAX(dbd.name) FROM line_relationship_vw lr "
           . "LEFT OUTER JOIN line_property_vw ad ON "
           . "(lr.object=ad.name AND lr.relationship='child_of' AND "
           . "ad.value='Split_GAL4-AD' AND ad.type='flycore_project') "
           . "LEFT OUTER JOIN line_property_vw dbd ON (lr.object=dbd.name "
           . "AND lr.relationship='child_of' AND dbd.value='Split_GAL4-DBD' "
           . "AND dbd.type='flycore_project') WHERE lr.subject=?",
DATASET => "SELECT DISTINCT value FROM image_vw i JOIN image_property_vw ip ON (i.id=ip.image_id AND ip.type='data_set') WHERE i.line=? AND i.family LIKE '%screen_review'",
HALVES => "SELECT value,name FROM line_property_vw lp JOIN line_relationship_vw lr ON (lr.object=lp.name AND lr.relationship='child_of') where type='flycore_project' AND lr.subject=?",
IMAGEM => "SELECT line,i.name,data_set,slide_code,area,cross_barcode,lpr.value "
          . "FROM image_data_mv i LEFT OUTER JOIN line_property_vw lpr ON "
          . "(i.line=lpr.name AND lpr.type='flycore_requester') "
          . "WHERE data_set LIKE ? "
          . "AND line LIKE 'JRC_IS%' ORDER BY 1 LIMIT 16",
IMAGES => "SELECT i.line,i.name,ipd.value,ips.value,ipa.value,ipc.value,"
          . "lpr.value,ipcs.value,ipp1.value,ipp2.value,"
          . "ipg1.value,ipg2.value "
          . "FROM image_vw i LEFT OUTER JOIN image_property_vw ipd "
          . "ON (i.id=ipd.image_id AND ipd.type='data_set') LEFT OUTER JOIN "
          . "image_property_vw ips ON (i.id=ips.image_id AND "
          . "ips.type='slide_code') LEFT OUTER JOIN image_property_vw ipa ON "
          . "(i.id=ipa.image_id AND ipa.type='area') LEFT OUTER JOIN "
          . "image_property_vw ipc ON (i.id=ipc.image_id AND "
          . "ipc.type='cross_barcode') LEFT OUTER JOIN line_property_vw lpr ON "
          . "(i.line=lpr.name AND lpr.type='flycore_requester') "
          . "LEFT OUTER JOIN image_property_vw ipcs ON (i.id=ipcs.image_id "
          . "AND ipcs.type='channel_spec') "
          . "LEFT OUTER JOIN image_property_vw ipp1 ON (i.id=ipp1.image_id "
          . "AND ipp1.type='lsm_illumination_channel_1_power_bc_1') "
          . "LEFT OUTER JOIN image_property_vw ipp2 ON (i.id=ipp2.image_id "
          . "AND ipp2.type='lsm_illumination_channel_2_power_bc_1') "
          . "LEFT OUTER JOIN image_property_vw ipg1 ON (i.id=ipg1.image_id "
          . "AND ipg1.type='lsm_detection_channel_1_detector_gain') "
          . "LEFT OUTER JOIN image_property_vw ipg2 ON (i.id=ipg2.image_id "
          . "AND ipg2.type='lsm_detection_channel_2_detector_gain') "
          . "WHERE ipd.value LIKE ? AND line LIKE 'JRC_IS%' ORDER BY 1",
ROBOT => "SELECT robot_id FROM line_vw WHERE name=?",
USERS => "SELECT DISTINCT(value) FROM image_property_vw WHERE "
         . "type='data_set' AND value LIKE '%screen_review'",
# ----------------------------------------------------------------------------
WS_LSMMIPSp => "SELECT eds.value,edm.value FROM entity e JOIN entityData edl ON (e.id=edl.parent_entity_id AND entity_att='Entity') JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Signal MIP Image') JOIN entityData edm ON (e.id=edm.parent_entity_id AND edm.entity_att='All MIP Image') JOIN entity el ON (edl.child_entity_id=el.id) WHERE el.name=?",
WS_LSMMIPS => "SELECT eds.value,edm.value FROM entity e JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Signal MIP Image') JOIN entityData edm ON (e.id=edm.parent_entity_id AND edm.entity_att='Default Fast 3D Image') WHERE e.name=?",
);
my $CLEAR = div({style=>'clear:both;'},NBSP);
my %username;

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
our $USERID = $Session->param('user_id');
our $USERNAME = $Session->param('user_name');
my $HEIGHT = param('height') || 150;
my $AUTHORIZED = (($Session->param('scicomp'))
                  || ($Session->param('flylight_split_screen')));
my $CAN_ORDER = ($AUTHORIZED) ? 0 : 1;

our ($dbh,$dbhw);
# Connect to databases
&dbConnect(\$dbh,'sage')
  || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
&dbConnect(\$dbhw,'workstation')
  || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
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
our $service = JFRC::LDAP->new();

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
elsif ($AUTHORIZED && !param('_userid') && !param('user') && !param('choose')) {
  &limitSearch();
}
else {
  &chooseCrosses();
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

sub limitSearch
{
  $sth{USERS}->execute();
  my $ar = $sth{USERS}->fetchall_arrayref();
  my %label = map {$a = (split('_',$_->[0]))[0];
                   my $user = $service->getUser($a);
                   $a => &getUsername($a)} @$ar;
  $label{''} = '(Any)';
  my $type = {'' => '(Any)',
              split => 'Split screen',
              ti => 'Terra incognita'};
  print div({class => 'boxed'},
            table({class => 'basic'},
                  Tr(td('User:'),
                     td(popup_menu(&identify('user'),
                                   -values => [sort keys %label],
                                   -labels => \%label))),
                  Tr(td('Image type:'),
                     td(popup_menu(&identify('type'),
                                   -values => ['','split','ti'],
                                   -labels => $type)))),
            div({align => 'center'},
                submit({&identify('choose'),
                        class => 'btn btn-success',
                        value => 'Search'}))
           ),br;
}


sub chooseCrosses
{
  my %lines = ();
  my ($class,$controls,$lhtml,$imagery,$last_line) = ('')x5;
  my $DSUSER = $USERID;
  my $DSTYPE = '%';
  if ($AUTHORIZED) {
    $DSUSER = param('_userid') || param('user') || '%';
    $DSTYPE = param('type') if (param('type'));
  }
  my $ds = $DSUSER . '\_' . $DSTYPE . '\_screen\_review%';
  $sth{IMAGES}->execute($ds);
  my $ar = $sth{IMAGES}->fetchall_arrayref();
  unless (scalar @$ar) {
    print &bootstrapPanel('No screen imagery found',
                          'No screen imagery was found'
                          . (($DSUSER eq '%') ? '.' : " for $DSUSER")
                          . br . "Search term: $ds",'danger');
    return;
  }
  my $html = '';
  my $ordered = 0;
  my $json = JSON->new->allow_nonref;
  foreach my $l (@$ar) {
    # Line, image name, data set, slide code, area, cross barcode, requester,
    # channel spec
    my($line,$name,$dataset,$slide,$area,$barcode,$requester,
       $chanspec,$power1,$power2,$gain1,$gain2) = @$l;
    $lines{$line}++;
    if ($line ne $last_line) {
      $html .= &renderLine($last_line,$lhtml,$imagery,$controls,$class) if ($lhtml);
      $sth{HALVES}->execute($line);
      my $hr = $sth{HALVES}->fetchall_hashref('value');
      my $type = ($dataset =~ /_ti_/) ? 'Terra incognita' : 'Split screen';
      $lhtml = h3(&lineLink($line) . (NBSP)x5 . $type);
      my $bh;
      if ($barcode) {
        $bh = a({href => "/flyboy_search.php?cross=$barcode",
                 target => '_blank'},$barcode);
      }
      else {
        $bh = '';
      }
      my @row = Tr(td(['Cross barcode:',$bh]));
      push @row,Tr(td(['Data set:',$dataset])) if ($AUTHORIZED);
      $requester = &getUsername((split('_',$dataset))[0])
        if ($AUTHORIZED && !param('user') && !$requester);
      push @row,Tr(td(['Requester:',$requester])) if ($requester);
      $lhtml .= table({class => 'basic'},@row);
      $lhtml .= join(br,table({class => 'halves'},
                             map {Tr(th($_.':'),td(&lineLink($hr->{$_}{name})))}
                                 sort keys %$hr)) if (scalar(keys %$hr));
      $imagery = '';
      $last_line = $line;
      $class = 'unordered';
      if ($barcode && (scalar(keys %$hr) == 2)) {
        my %request;
        my $client = REST::Client->new();
        # Development $client->GET("http://10.102.20.190:8000/api/orders/is/$line/");
        $client->GET("http://$FLYSTORE_HOST/api/orders/is/$line/");
        my $struct = $json->decode($client->responseContent());
        foreach my $order (keys %$struct) {
          foreach (@{$struct->{$order}{crossTypes}}) {
            $request{$_} = $struct->{$order}{dateCreated};
          }
        }
        $controls = div({class => 'checkboxes'},
                        table({style => 'margin-right: 10px'},
                              map {my $c = join('_',$line,lc($_),'cross');
                                   if (exists $request{lc($_)}) {
                                     $class = 'ordered';
                                     $ordered++;
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
                                            onclick => 'tagCross("'.$line.'");'},$_),
                                     (/Stabilization/) ? ''
                                     : input({&identify(join('_',$line,lc($_),'pri')),
                                              type => 'checkbox',
                                              class => 'lineselect'},'High priority')]))
                                   }
                                  } @CROSS));
      }
      else {
        $controls = div({class => 'checkboxes',style => 'color: #000'},
                        &bootstrapPanel('Cannot order stable splits',
                                        (($barcode) ? "$line doesn't have two split halves"
                                                    : "$line has no cross barcode"),'danger'));
      }
    }
    $name =~ s/.+\///;
    $name =~ s/\.bz2//;
    $sth{LSMMIPS}->execute($name);
    my($signal,$reference) = $sth{LSMMIPS}->fetchrow_array();
    (my $i = $signal) =~ s/.+filestore\///;
    if ($i) {
      $i = "/imagery_links/ws_imagery/$i";
      $signal = a({href => "http://jacs-webdav.int.janelia.org/WebDAV".$signal,
                   target => '_blank'},
                  img({src => $i, height => $HEIGHT}));
    }
    ($i = $reference) =~ s/.+filestore\///;
    if ($i) {
      $i = "/imagery_links/ws_imagery/$i";
      $reference = a({href => "http://jacs-webdav.int.janelia.org/WebDAV".$reference,
                      target => '_blank'},
                     img({src => '/images/stack.png',
                          title => 'Show movie'}));
    }
    my $pgv = 'Unknown power/gain';
    my $format = "P&times;G %.2f&times;%d (%.2f)";
    if (!index($chanspec,'s')) {
      $pgv = sprintf $format,$power1/100,$gain1,($power1/100)*$gain1
        if ($power1 && $gain1);
    }
    else {
      $pgv = sprintf $format,$power2/100,$gain2,($power2/100)*$gain2
        if ($power2 && $gain2);
    }
    $imagery .= div({class => 'single_mip'},$signal,br,
                    table(Tr(td({width => '10%'},NBSP),
                             td({width => '80%'},$area),
                             td({class => 'imgoptions'},$reference)),
                          Tr(td({colspan => 3},$pgv)),
                         )
                   );
  }
  $html .= &renderLine($last_line,$lhtml,$imagery,$controls,$class) if ($lhtml);
  my $uname = $USERNAME;
  $uname .= ' (running as ' . &getUsername(param('_userid')) . ')'
    if ($AUTHORIZED && param('_userid'));
  my @other;
  if ($AUTHORIZED) {
    if (param('user')) {
      push @other,Tr(td(['Imaged for: ',&getUsername(param('user'))]));
      push @other,Tr(td(['Image type: ',(param('type') eq 'ti') ? 'Terra incognita' : 'Split screen'])) if param('type');
    }
  }
  print div({class => 'boxed'},
            table({class => 'standard'},
                  Tr(td(['User: ',$uname])),
                  @other,
                  Tr(td(['Lines found: ',scalar keys %lines])),
                  Tr(td(['Lines already ordered: ',$ordered])),
                  (map { Tr(td(["$_ crosses requested:",
                                div({class => lc($_).'_crosses'},0),
                               ]))
                       } @CROSS),
                 ),
            button(-value => 'Show all lines',
                   -class => 'btn btn-primary btn-xs',
                   -onclick => 'showAll();'),
            button(-value => 'Hide unchecked lines',
                   -class => 'btn btn-primary btn-xs',
                   -onclick => 'hideUnchecked();'),
            button(-value => 'Hide checked lines',
                   -class => 'btn btn-primary btn-xs',
                   -onclick => 'hideChecked();'),
            button(-value => 'Hide ordered lines',
                   -class => 'btn btn-primary btn-xs',
                   -onclick => 'hideByClass("ordered");'),
            button(-value => 'Hide unordered lines',
                   -class => 'btn btn-primary btn-xs',
                   -onclick => 'hideByClass("unordered");'),
            div({align => 'center'},
                submit({&identify('verify'),
                        class => 'btn btn-success',
                        value => 'Next >'}))
           ),br,
        div({id => 'scrollarea'},$html);
}


sub lineLink
{
  my $l = shift;
  a({href => 'lineman.cgi?line='.$l,
     target => '_blank'},$l);
}


sub renderLine {
  my($line,$html,$imagery,$controls,$class) = @_;
  div({class => "line $class",
       id => $line},
      div({float => 'left'},$html,
          div({class => 'inputblock'},$imagery,$controls)),
      $CLEAR);
}


sub verifyCrosses
{
  my %line;
  my $total = 0;
  my ($control,$priority) = ('')x2;
  foreach (param()) {
    $control .= hidden(&identify($_),default => param($_))
      unless ($_ eq 'verify');
    if (/cross$/) {
      $line{join('_',(split('_'))[0,1])} = param($_);
      $total++;
    }
    elsif (/pri$/) {
      $priority = 'Red check marks indicate high-priority crosses.' . br;
    }
  }
  my $normal = '<span class="glyphicon glyphicon-ok" aria-hidden="true"></span>';
  my $high = '<span class="glyphicon glyphicon-ok" style="color: red" aria-hidden="true"></span>';
  my $rbutton = submit({&identify('request'),
                       class => 'btn btn-success',
                       value => 'Request crosses'});
  $rbutton = '' unless ($CAN_ORDER);
  print table({class => 'verify'},
              thead(Tr(th(['Line','Cross barcode',@CROSS]))),
              tbody(map {my @col;
                         foreach my $c (@CROSS) {
                           push @col,(param(join('_',$_,lc($c),'cross')))
                                     ? ((param(join('_',$_,lc($c),'pri'))) ? $high : $normal)
                                     : NBSP;
                         }
                         Tr(th($_),td([$line{$_},@col]));
                        } sort keys %line)),
        br,
        (sprintf 'A total of %d cross%s will be ordered for %s.',
                 $total,(1 == $total) ? '' : 'es',$USERNAME),br,
        $priority,
        div({align => 'center'},
                submit({&identify('cancel'),class => 'btn btn-danger',
                        value => "Cancel",
                        onclick => 'window.location.href="ssplit_review.cgi"'}),
                NBSP,$rbutton),
        $control;
}


sub requestCrosses
{
  my %type;
  my @row;
  my $head = ['Line','Cross ID','AD line','AD robot ID','DBD line','DBD robot ID',
              'Cross type','Priority','Requester'];
  my %line;
  my $total = 0;
  foreach (param()) {
    if (/cross$/) {
      $line{my $l = join('_',(split('_'))[0,1])} = param($_);
      $total++;
      unless ($type{$l}) {
        $sth{DATASET}->execute($l);
        my $ar = $sth{DATASET}->fetchall_arrayref();
        $type{$l} = ($ar->[0][0] =~ /_ti_/) ? 'Terra incognita' : 'Split screen';
      }
    }
  }
  print "Crosses to be ordered: $total" . (br)x2;
  my $json = JSON->new->allow_nonref;
  my (%error,%success);
  foreach my $line (sort keys %line) {
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
      my $order = {username => $USERID,
                   splits => [@splits],
                   specialInstructions => $type{$line},
                   createNewOrder => 0};
      my $json_text = $json->encode($order);
      my $client = REST::Client->new();
      $client->POST("http://$FLYSTORE_HOST/api/order/",$json_text);
      if ($client->responseCode() == 201) {
        $success{$line}++;
      }
      else {
        $error{$line} = $json_text;
      }
    }
  }
  if (scalar keys %success) {
    print &bootstrapPanel('Lines ordered',join(', ',sort keys %success),
                          'success');
  }
  if (scalar keys %error) {
    print &bootstrapPanel('Could not request the following crosses:',
                          join(br,map {$_ . (NBSP)x5 . $error{$_}} sort keys %error),
                          'danger');
  }
  print &createExportFile(\@row,'_'.$USERID.'_cross_request',$head);
}


sub getUsername
{
  my $userid = shift;
  return($username{$userid}) if (exists $username{$userid});
  my $user = $service->getUser($userid);
  $username{$userid} = join(' ',$user->givenName(),$user->sn());
  return($username{$userid});
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
                    (qw(chosen.jquery.min jquery/jquery.tablesorter tablesorter),$PROGRAM);
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1 chosen.min);
  $load{load} = ' tableInitialize();';
  &standardHeader(title       => $arg{title},
                  css_prefix  => $PROGRAM,
                  script      => \@scripts,
                  style       => \@styles,
                  breadcrumbs => \@BREADCRUMBS,
                  expires     => 'now',
                  %load);
}
