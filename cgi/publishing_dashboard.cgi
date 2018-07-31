#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Date::Calc qw(Add_Delta_Days);
use Date::Manip qw(UnixDate);
use DBI;
use JSON;
use LWP::Simple qw(get);
use XML::Simple;
use JFRC::LDAP;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);
use lib '/groups/scicompsoft/home/svirskasr/workspace/JFRC-Utils-FLEW/lib';
use JFRC::Utils::FLEW qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Publishing dashboard';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my %BG = (Inactive => '#999',
          'Annotation in progress' => '#96f',
          Annotated => '#69f',
          'Pre-staged' => '#bb0',
          Staged => '#c90',
          Production => '#696');
my %header = (SUMMARY => ['Line','Images'],
              DETAIL => ['Line','Image ID','Name','Objective','Area','Tile',
                         'Slide code','LSMs','Projections']);
my $MEASUREMENT_DAYS = 30;
my $CONFIG_SERVER = 'http://config.int.janelia.org/config';
my %CONFIG;

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our (%pdbh,%sth);
our $dbh;
# Counters
my (%image_check,%line_check);
my $release_count;

# ****************************************************************************
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
my %sths = (
  ANNOTATIONS => "SELECT DISTINCT i.line,alps_release,GROUP_CONCAT(DISTINCT "
                 . "o.type SEPARATOR ', ') FROM image_data_mv i LEFT OUTER "
                 . "JOIN session_vw s ON (i.line=s.line) LEFT OUTER JOIN "
                 . "observation_vw o ON (s.id=o.session_id) WHERE "
                 . "i.to_publish='Y' GROUP BY 1,2 ORDER BY 1,2",
  LINES => "SELECT COUNT(DISTINCT line) FROM image_data_mv "
           . "WHERE published='Y'",
  PUBLISHEDSG => "SELECT published_to,alps_release,COUNT(DISTINCT line),"
                 . "COUNT(1) FROM image_data_mv WHERE to_publish='Y' "
                 . "GROUP BY 1,2 ORDER BY 1,2",
  PUBLISHED => "SELECT published_to,alps_release,COUNT(DISTINCT line),"
               . "COUNT(1) FROM image_data_mv WHERE to_publish='Y' OR published='Y' "
               . "GROUP BY 1,2 ORDER BY 1,2",
  STAGED => "SELECT alps_release,COUNT(DISTINCT line),COUNT(1) FROM "
            . "image_data_mv WHERE to_publish='Y' AND published='Y' AND "
            . "published_externally IS NULL GROUP BY 1",
  WAITING => "SELECT line,publishing_name,published_to,alps_release,publishing_user,"
             . "GROUP_CONCAT(DISTINCT IFNULL(tile,'NULL') ORDER BY tile SEPARATOR ', '),"
             . "GROUP_CONCAT(DISTINCT objective SEPARATOR ', '),"
             . "GROUP_CONCAT(DISTINCT effector SEPARATOR ', '),COUNT(1) FROM "
             . "image_data_mv WHERE published IS NULL "
             . "AND to_publish='Y' GROUP BY 1,2,3,4",
  SPLIT_GAL4 => "SELECT COUNT(DISTINCT line) FROM image_data_mv WHERE "
               . "published='Y' AND published_to='Split GAL4'",
  SUMMARY => "SELECT line,COUNT(1) FROM image_data_mv WHERE "
             . "alps_release=? group by 1",
  DETAIL => "SELECT i.line,i.id,i.name,i.objective,i.area,i.tile,i.slide_code,"
            . "IF(i2.url IS NULL,'','Yes') AS LSM,IF(COUNT(s.id) > 0,'Yes','') "
            . "AS Proj,workstation_sample_id FROM image_data_mv i JOIN image i2 ON (i.id=i2.id) "
            . "LEFT JOIN secondary_image s ON (s.image_id=i.id) "
            . "WHERE alps_release=? GROUP BY 2 ORDER BY 1,4,5",
);
my %FLEW = (
  PUBLISHED => "SELECT COUNT(DISTINCT line),COUNT(1) FROM image_data_mv "
               . "WHERE family != 'rubin_lab_external'",
);
my %MBEW = (
  ANNOTATIONS => "SELECT DISTINCT i.line,alps_release,GROUP_CONCAT(DISTINCT "
                 . "term SEPARATOR ', ') FROM image_data_mv i LEFT OUTER "
                 . "JOIN session_vw s ON (i.line=s.line) LEFT OUTER JOIN "
                 . "observation_vw o ON (s.id=o.session_id) "
                 . "GROUP BY 1,2 ORDER BY 1,2",
  LINES => "SELECT COUNT(DISTINCT line) FROM image_data_mv",
  PUBLISHED => "SELECT alps_release,COUNT(DISTINCT line),COUNT(1) FROM "
               . "image_data_mv GROUP BY 1",
  HALVES => "SELECT value,COUNT(1) FROM line l JOIN line_property_vw lp ON "
            . "(l.id=lp.line_id AND lp.type='flycore_project') WHERE "
            . "l.name NOT IN (SELECT line FROM image_data_mv) GROUP BY 1",
  SUMMARY => "SELECT line,COUNT(1) FROM image_data_mv WHERE "
             . "alps_release=? group by 1",
  DETAIL => "SELECT i.line,i.id,i.name,i.objective,i.area,i.tile,i.slide_code,"
            . "IF(i2.url IS NULL,'','Yes') AS LSM,IF(COUNT(s.id) > 0,'Yes','') "
            . "AS Proj,workstation_sample_id FROM image_data_mv i JOIN image i2 ON (i.id=i2.id) "
            . "LEFT JOIN secondary_image s ON (s.image_id=i.id) "
            . "WHERE alps_release=? GROUP BY 2 ORDER BY 1,4,5",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
if (param('release')) {
  &displayRelease(param('release'),param('instance'));
}
elsif (param('report')) {
  &releaseReport(param('report'),param('instance'));
}
elsif (param('annotations')) {
  &displayAnnotations(param('instance'));
}
else {
  &displayDashboard();
}
# We're done!
if ($dbh) {
  ref($sths{$_}) && $sths{$_}->finish foreach (keys %sths);
  $dbh->disconnect;
  foreach my $i (keys %sth) {
    ref($sth{$i}{$_}) && $sth{$i}{$_}->finish foreach (keys %{$sth{$i}});
    $pdbh{$i}->disconnect;
  }
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub getREST
{
  my($server,$endpoint) = @_;
  my $url = $CONFIG{$server}{url} . $endpoint;
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
  # Get general REST config
  my $rest = $CONFIG_SERVER . '/rest_services';
  my $response = get $rest;
  my $rvar;
  eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
  %CONFIG = %{$rvar->{config}};
  # Connect to databases
  &dbConnect(\$dbh,'sage');
  $sths{$_} = $dbh->prepare($sths{$_}) || &terminateProgram($dbh->errstr)
    foreach (keys %sths);
  foreach my $i ('flew-dev','flew-prod') {
    print STDERR "Connect to $i\n";
    &dbConnect(\$pdbh{$i},split('-',$i));
    $sth{$i}{$_} = $pdbh{$i}->prepare($FLEW{$_}) || &terminateProgram($pdbh{$i}->errstr)
      foreach (keys %FLEW);
  }
  foreach my $i ('mbew-dev','mbew-prod') {
    print STDERR "Connect to $i\n";
    &dbConnect(\$pdbh{$i},split('-',$i));
    $sth{$i}{$_} = $pdbh{$i}->prepare($MBEW{$_}) || &terminateProgram($pdbh{$i}->errstr)
      foreach (keys %MBEW);
  }
}


sub queryAnnotations
{
  my($instance) = shift;
  my $ar;
  if ($instance) {
    $sth{$instance}{ANNOTATIONS}->execute();
    $ar = $sth{$instance}{ANNOTATIONS}->fetchall_arrayref();
  }
  else {
    $sths{ANNOTATIONS}->execute();
    $ar = $sths{ANNOTATIONS}->fetchall_arrayref();
  }
  return($ar);
}


sub getTitle
{
  my $title = shift;
  my $t = param('title');
  if ($t) {
    $title .= " ($t)";
    $title = span({style => "background: $BG{$t}"},$title);
  }
  return(h1($title));
}


sub displayAnnotations
{
  my($instance) = @_;
  &printHeader();
  my $display;
  my @header = ['Line','ALPS release','Annotations'];
  my $ar = &queryAnnotations($instance);
  foreach (@$ar) {
    $_->[-1] = &greekify($_->[-1]);
  }
  $display .= table({id => 'annotations',class => 'tablesorter standard'},
                    thead(Tr(th(@header))),
                    tbody(map {Tr(td($_))} @$ar)) . br;
  print &getTitle('Annotations'),$display,
        end_form,&sessionFooter($Session),end_html;
}


sub releaseReport
{
  my($release,$instance) = @_;
  &printHeader();
  print &getTitle("Release report for $release"),br;
  my $ar;
  if ($instance) {
    $sth{$instance}{DETAIL}->execute($release);
    $ar = $sth{$instance}{DETAIL}->fetchall_arrayref();
  }
  else {
    $sths{DETAIL}->execute($release);
    $ar = $sths{DETAIL}->fetchall_arrayref();
  }
  my %line;
  my($image_count) = (0)x0;
  my(@lsm,@mip);
  foreach (@$ar) {
    $line{$_->[0]}++;
    $image_count++;
    my $sid = pop @$_;
    $_->[2] = a({href => "http://webstation.int.janelia.org/do/$sid",
                 target => '_blank'},$_->[2]) if ($sid);
    push @lsm,$_ unless ($_->[6]);
    push @mip,$_ unless ($_->[7]);
  }
  (my $rel = $release) =~ s/&/%26/g;
  my $summary = 'Lines: ' . scalar(keys %line) . br
                . 'Images: ' . $image_count . br
                . a({href => "?release=$rel;instance=$instance;title=" . param('title'),
                   target => '_blank'},'Show details');
  print &bootstrapPanel('Release summary',$summary,'info');
  if (scalar @lsm) {
    print &bootstrapPanel('Images with missing LSMs',
                          table({id => 'lsm',class => 'tablesorter standard'},
                          thead(Tr(th($header{DETAIL}))),
                          tbody(map {Tr(td($_))} @lsm)),'warning');
                          
  }
  if (scalar @mip) {
    print &bootstrapPanel('Images with missing MIPs',
                          table({id => 'mip',class => 'tablesorter standard'},
                          thead(Tr(th($header{DETAIL}))),
                          tbody(map {Tr(td($_))} @mip)));
                          
  }
  $ar = &queryAnnotations($instance);
  my @ann;
  foreach (@$ar) {
    push @ann,$_->[0] if (($_->[1] eq $release) && (!$_->[2]));
  }
  if (scalar @ann) {
    print &bootstrapPanel('Lines with missing annotations',
                          ((scalar(keys %line) == scalar(@ann)) ? 'No lines have annotations'
                                                                : join(', ',sort @ann)));
  }
  print &bootstrapPanel('Validated','This release has no missing data','success')
    unless (scalar(@mip) || scalar(@lsm) || scalar(@ann));
  print end_form,&sessionFooter($Session),end_html;
}


sub displayRelease
{
  my($release,$instance) = @_;
  &printHeader();
  my $display;
  foreach my $cursor('SUMMARY','DETAIL') {
    my $ar;
    if ($instance) {
      $sth{$instance}{$cursor}->execute($release);
      $ar = $sth{$instance}{$cursor}->fetchall_arrayref();
    }
    else {
      $sths{$cursor}->execute($release);
      $ar = $sths{$cursor}->fetchall_arrayref();
    }
    my $image_count;
    $image_count += $_->[1] foreach (@$ar);
    if ($cursor eq 'DETAIL') {
      foreach (@$ar) {
        my $sid = pop @$_;
        $_->[2] = a({href => "http://webstation.int.janelia.org/do/$sid",
                     target => '_blank'},$_->[2]);
      }
    }
    $display .= table({id => '$cursor',class => 'tablesorter standard'},
                      thead(Tr(th($header{$cursor}))),
                      tbody(map {Tr(td($_))} @$ar),
                      ($cursor eq 'SUMMARY') ? tfoot(Tr(td(['',$image_count]))) : '') . br;
  }
  print &getTitle("$release details"),$display,
        end_form,&sessionFooter($Session),end_html;
}


sub displayDashboard
{
  &printHeader();
  my $waiting = '';
  my %published;
  ($published{'Pre-staged'},$waiting) = &getPrestagedData();
  $published{Staged} = &getStagedData('mbew-dev');
  $published{Staged} .= &getStagedData('flew-dev');
  $published{Production} = &getStagedData('mbew-prod');
  $published{Production} .= &getStagedData('flew-prod');
  my $detail = '';
  # Render
  if ($waiting) {
    $detail .= &bootstrapPanel('On SAGE, awaiting publishing',
                               $waiting,'info')
               . div({style => 'clear: both;'},NBSP);
  }
  my $render = '';
  my $h = sprintf '%dpx',400 + 30 * $release_count;
  foreach ('Pre-staged','Staged','Production') {
    $render .= div({class => 'publish',
                    style => "height: $h;background-color: $BG{$_};"},
                   h1({class => 'boxhead'},$_),$published{$_});
  }
  $detail .= &bootstrapPanel('Published',$render,'success');
  my @content = ({id => 'summary', title => 'Summary', content => &ALPSSummary()},
                 {id => 'detail', title => 'Detail', content=>$detail});
  my($class,$class2) = ('active','tab-pane active');
  print div({role=>'tabpanel'},
            ul({class=>'nav nav-tabs',role=>'tablist'},
               map {$a = li({role=>'presentation',class=>$class},
                            a({href=>'#'.$_->{id},
                               'aria-controls'=>$_->{id},
                               role=>'tab',
                               'data-toggle'=>'tab'},$_->{title}));
                    $class = '';
                    $a;
                   } @content),br,
            div({class=>'tab-content'},
                map {$a = div({role=>'tabpanel',class=>$class2,id=>$_->{id}},
                              $_->{content});
                     $class2 = 'tab-pane';
                     $a} @content)
           );
  print end_form,&sessionFooter($Session),end_html;
}


sub getAnnotations
{
  my($arrayref,$instance,$current) = @_;
  my($lc,$ac) = (0)x2;
  foreach (@$arrayref) {
    $lc++;
    $ac++ if ($_->[2]);
  }
  my $msg = "$ac of $lc Split GAL4 lines have annotations";
  span({style => 'background: white'},
       a({href => "?annotations=1&instance=$instance&title=$current",
          target => '_blank'},$msg));
}


sub ALPSSummary
{
  # Production
  my $instance = 'mbew-prod';
  $sth{$instance}{PUBLISHED}->execute();
  my $ar = $sth{$instance}{PUBLISHED}->fetchall_arrayref();
  my %step;
  foreach (@$ar) {
    $step{Production}{$_->[0]}{lines} = $_->[1];
    $step{Production}{$_->[0]}{images} = $_->[2];
  }
  # Staged
  @$ar = ();
  $sths{STAGED}->execute();
  $ar = $sths{STAGED}->fetchall_arrayref();
  foreach (@$ar) {
    next if (exists $step{Production}{$_->[0]});
    $step{Staged}{$_->[0]}{lines} = $_->[1];
    $step{Staged}{$_->[0]}{images} = $_->[2];
  }
  # Pre-staged
  $sths{PUBLISHEDSG}->execute();
  $ar = $sths{PUBLISHEDSG}->fetchall_arrayref();
  foreach (@$ar) {
    next if (exists $step{Production}{$_->[1]} || exists $step{Staged}{$_->[1]});
    $step{'Pre-staged'}{$_->[1]}{lines} = $_->[2];
    $step{'Pre-staged'}{$_->[1]}{images} = $_->[3];
  }
  # Annotation
  my $resp = &getREST('jacs',"process/release");
  my %alc;
  foreach my $r (@$resp) {
    my $name = $r->{name};
    next if (exists $step{Production}{$name});
    my $use_step = ($r->{sageSync}) ? 'Annotated' : 'Annotation in progress';
    if (!$r->{sageSync} && $r->{updatedDate}) {
      my $updated = (split('T',$r->{updatedDate}))[0];
      my $today = UnixDate("today","%Y-%m-%d");
      my $ago = sprintf '%4d-%02d-%02d',
                        Add_Delta_Days(split('-',$today),-$MEASUREMENT_DAYS);
      $use_step = 'Inactive' if ($updated < $ago);
    }
    my $rel = &getREST('jacs',"process/release/$name/status");
    foreach (keys %$rel) {
      $alc{$_}++;
      $step{$use_step}{$name}{images} += $rel->{$_}{numSamples};
    }
    $step{$use_step}{$name}{lines} = scalar(keys %alc);
    %alc = ();
  }
  my @block;
  my @steps = ('Annotated','Pre-staged','Staged','Production');
  if (exists $step{'Inactive'}) {
    unshift @steps,'Inactive','Annotation in progress';
  }
  elsif (exists $step{'Annotation in progress'}) {
    unshift @steps,'Annotation in progress';
  }
  foreach my $s (@steps) {
    my $inner;
    my $h = sprintf '%dpx',32 + 22 * scalar(keys %{$step{$s}});
    foreach (sort keys %{$step{$s}}) {
      my $label = ($s =~ /^(?:Inactive|Annot)/) ? 'sample' : 'image';
      $inner .= (sprintf "%s (%d line%s, %d %s%s",
                 $_,scalar($step{$s}{$_}{lines}),
                 (scalar($step{$s}{$_}{lines}) == 1 ? '' : 's'),
                 scalar($step{$s}{$_}{images}),$label,
                 (scalar($step{$s}{$_}{images}) == 1 ? '' : 's')) . ')<br>';
    }
    push @block,div({class => 'step',
                    style => "height: $h;background-color: $BG{$s};"},
                   h1({class => 'boxhead'},$s),span({style => 'color: black'},$inner));
  }
  my $divider = '<span class="glyphicon glyphicon-arrow-down" aria-hidden="true"></span>';
  return(div({align => 'center', style => 'margin: 0 auto;'},
             join($divider,@block)));
}


sub linkLine
{
  return(a({href => "lineman.cgi?line=$_->[0]",
            target => '_blank'},$_->[0]));
}


sub getPrestagedData
{
  my $service = JFRC::LDAP->new({host => 'ldap-vip3.int.janelia.org'});
  my ($annotator,$ar,$waiting) = ('')x3;
  # Waiting
  $sths{WAITING}->execute();
  $ar = $sths{WAITING}->fetchall_arrayref();
  if (scalar @$ar) {
    my %line;
    my($images,$err) = (0)x2;
    foreach (@$ar) {
      $line{$_->[0]}++;
      $_->[0] = &linkLine($_->[0]);
      unless ($_->[2]) {
        $_->[2] = 'FLEW';
        $_->[3] = '(FLEW)';
      }
      if ($_->[4]) {
        my $u = $service->getUser($_->[4]);
        $annotator = join(' ',$u->{givenName},$u->{sn});
      }
      $_->[4] = ($annotator ne ' ') ? $annotator : $_->[4];
      my @o;
      foreach my $o (split(', ',$_->[6])) {
        $o =~ s/\D+(\d+[Xx]).+/$1/;
        push @o,$o;
      }
      $_->[5] = join(', ',@o);
      $images += $_->[-1];
      if (!$_->[1]) {
        $_->[0] = span({style => 'border: 1px solid red'},$_->[0]);
        $err++;
      }
      splice(@$_,1,1);
    }
    $waiting = 'Lines enclosed in a '
               . span({style => 'border: 1px solid red'},'red box')
               . ' are missing publishing names.<br>' if ($err);
    $waiting .= table({id => 'waiting',class => 'tablesorter standard'},
                      thead(Tr(th(['Line','Website','ALPS release',
                                  'Annotator','Tiles','Objectives','Reporters','Images']))),
                     tbody(map {Tr(td($_))} @$ar),
                     tfoot(Tr(td([scalar keys(%line),('')x6,$images]))));
  }
  # Published
  $sths{PUBLISHED}->execute();
  $ar = $sths{PUBLISHED}->fetchall_arrayref();
  $release_count = scalar(@$ar);
  my $published;
  if (scalar @$ar) {
    $sths{LINES}->execute();
    my($line_count) = $sths{LINES}->fetchrow_array();
    my $image_count = 0;
    $image_count += $_->[-1] foreach (@$ar);
    my %group;
    foreach (@$ar) {
      if ($_->[0] =~ /^FLEW/) {
        $_->[1] = ($_->[0] eq 'FLEW') ? '(FLEW)' : '(FLEW-VT)';
        $_->[0] = 'FLEW';
      }
      $a = $_->[0];
      $_->[0] = a({href => (($_->[0] eq 'FLEW') ? 'http://www.janelia.org/gal4-gen1'
                                                : 'http://splitgal4.janelia.org'),
                   target => '_blank'},$_->[0]);
      $group{$_->[0]}{name} = $a;
      $group{$_->[0]}{lines} += $_->[2];
      $group{$_->[0]}{images} += $_->[3];
      $line_check{'Pre-staged'}{$_->[1]} = $_->[2];
      $image_check{'Pre-staged'}{$_->[1]} = $_->[3];
      unless ($_->[0] =~ /FLEW/) {
        (my $rel = $_->[1]) =~ s/&/%26/g;
        $_->[1] = a({href => "?report=$rel&title=Pre-staged",
                     target => '_blank'},$_->[1]);
      }
    }
    $sths{SPLIT_GAL4}->execute();
    foreach (keys %group) {
      $group{$_}{lines} = $sths{SPLIT_GAL4}->fetchrow_array() if (/Split GAL4/);
    }
    $sths{ANNOTATIONS}->execute();
    my($amsg) = &getAnnotations($sths{ANNOTATIONS}->fetchall_arrayref(),'','Pre-staged');
    $published = table({id => 'published',class => 'tablesorter standard'},
                       thead(Tr(th(['Website','ALPS release','Lines','Images']))),
                       tbody(map {Tr(td($_))} @$ar),
                       tfoot(Tr(td(['','',$line_count,$image_count]))))
                 . table({class => 'standard'},
                         thead(Tr(th(['Website','Lines','Images']))),
                         tbody(map {Tr(td([$_,$group{$_}{lines},$group{$_}{images}]))}
                                   sort {$group{$a}{name} cmp $group{$b}{name}} keys %group))
                 . br . $amsg;
  }
  return($published,$waiting);
}


sub getStagedData
{
  my $instance = shift;
  $sth{$instance}{PUBLISHED}->execute();
  my $ar = $sth{$instance}{PUBLISHED}->fetchall_arrayref();
  my $published = '';
  my($previous) = ($instance =~ /dev/) ? 'Pre-staged' : 'Staged';
  my($current) = ($instance =~ /dev/) ? 'Staged' : 'Production';
  if ($instance =~ /mbew/) {
    $sth{$instance}{LINES}->execute();
    my($line_count) = $sth{$instance}{LINES}->fetchrow_array();
    my $image_count = 0;
    foreach (@$ar) {
      $image_count += $_->[2];
      $line_check{$current}{$_->[0]} = $_->[1];
      $image_check{$current}{$_->[0]} = $_->[2];
      $_->[1] = span({class => 'mismatch'},$_->[1])
        if ($line_check{$previous}{$_->[0]} != $_->[1]);
      $_->[2] = span({class => 'mismatch'},$_->[2])
        if ($image_check{$previous}{$_->[0]} != $_->[2]);
      (my $rel = $_->[0]) =~ s/&/%26/g;
      $_->[0] = a({href => "?report=$rel;instance=$instance;title=$current",
                   target => '_blank'},$_->[0]);
    }
    $published = table({id => 'publishedm',class => 'tablesorter standard'},
                       thead(Tr(th(['ALPS release','Lines','Images']))),
                       tbody(map {Tr(td($_))} @$ar),
                       tfoot(Tr(td(['',$line_count,$image_count]))));
    $sth{$instance}{HALVES}->execute();
    $ar = $sth{$instance}{HALVES}->fetchall_arrayref();
    my $tsh = 0;
    $tsh += $_->[1] foreach (@$ar);
    $sth{$instance}{ANNOTATIONS}->execute();
    my($amsg) = &getAnnotations($sth{$instance}{ANNOTATIONS}->fetchall_arrayref(),$instance,$current);
    $published .= table({id => 'publishedh',class => 'tablesorter standard'},
                        thead(Tr(th(['Lines','Images']))),
                        tbody(map {Tr(td($_))} @$ar),
                        tfoot(Tr(td(['',$tsh]))))
                  . $amsg;
  }
  else {
    $published = table({id => 'publishedf',class => 'tablesorter standard'},
                       thead(Tr(th(['Lines','Images']))),
                       tbody(map {Tr(td($_))} @$ar));
  }
  my($header) = ($instance =~ /flew/) ? 'Gen1 GAL4/LexA' : 'Split-GAL4';
  return(h2($header) . $published);
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('jquery/jquery.tablesorter','tablesorter');
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
