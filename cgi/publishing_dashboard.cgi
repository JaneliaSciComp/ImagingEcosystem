#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use XML::Simple;
use JFRC::LDAP;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);
use JFRC::Highcharts qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';
my $BASE = "/var/www/html/output/";

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Publishing dashboard';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my %CONFIG;
use constant NBSP => '&nbsp;';
my %BG = ('Pre-staged' => '#bb0',
          Staged => '#c90',
          Production => '#696');

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
  LINES => "SELECT COUNT(DISTINCT line) FROM image_data_mv "
           . "WHERE published='Y'",
  PUBLISHED => "SELECT published_to,alps_release,COUNT(DISTINCT line),"
               . "COUNT(1) FROM image_data_mv WHERE published='Y' "
               . "GROUP BY 1,2 ORDER BY 1,2",
  WAITING => "SELECT line,published_to,alps_release,publishing_user,"
             . "GROUP_CONCAT(DISTINCT IFNULL(tile,'NULL') ORDER BY tile SEPARATOR ', '),"
             . "GROUP_CONCAT(DISTINCT objective SEPARATOR ', '),"
             . "GROUP_CONCAT(DISTINCT effector SEPARATOR ', '),COUNT(1) FROM "
             . "image_data_mv WHERE published IS NULL "
             . "AND to_publish='Y' GROUP BY 1,2,3,4",
  SPLIT_GAL4 => "SELECT COUNT(DISTINCT line) FROM image_data_mv WHERE "
               . "published='Y' AND published_to='Split GAL4'",
  SUMMARY => "SELECT line,COUNT(1) FROM image_data_mv WHERE "
             . "alps_release=? group by 1",
  DETAIL => "SELECT i.line,i.id,i.name,i.area,i.tile,i.slide_code,"
            . "IF(i2.url IS NULL,'','Yes') AS LSM,IF(COUNT(s.id) > 0,'Yes','') "
            . "AS Proj FROM image_data_mv i JOIN image i2 ON (i.id=i2.id) "
            . "LEFT JOIN secondary_image s ON (s.image_id=i.id) "
            . "WHERE alps_release=? GROUP BY 2 ORDER BY 1,4,5",
);
my %FLEW = (
  PUBLISHED => "SELECT COUNT(DISTINCT line),COUNT(1) FROM image_data_mv "
               . "WHERE family != 'rubin_lab_external'",
);
my %MBEW = (
  LINES => "SELECT COUNT(DISTINCT line) FROM image_data_mv",
  PUBLISHED => "SELECT alps_release,COUNT(DISTINCT line),COUNT(1) FROM "
               . "image_data_mv GROUP BY 1",
  HALVES => "SELECT value,COUNT(1) FROM line l JOIN line_property_vw lp ON "
            . "(l.id=lp.line_id AND lp.type='flycore_project') WHERE "
            . "l.name NOT IN (SELECT line FROM image_data_mv) GROUP BY 1",
  SUMMARY => "SELECT line,COUNT(1) FROM image_data_mv WHERE "
             . "alps_release=? group by 1",
  DETAIL => "SELECT i.line,i.id,i.name,i.area,i.tile,i.slide_code,"
            . "IF(i2.url IS NULL,'','Yes') AS LSM,IF(COUNT(s.id) > 0,'Yes','') "
            . "AS Proj FROM image_data_mv i JOIN image i2 ON (i.id=i2.id) "
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

sub initializeProgram
{
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


sub displayRelease
{
  my($release,$instance) = @_;
  &printHeader();
  my $display;
  my %header = (SUMMARY => ['Line','Images'],
                DETAIL => ['Line','Image ID','Name','Area','Tile','Slide code','LSMs','Projections']);
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
    $display .= table({id => '$cursor',class => 'tablesorter standard'},
                      thead(Tr(th($header{$cursor}))),
                      tbody(map {Tr(td($_))} @$ar),
                      ($cursor eq 'SUMMARY') ? tfoot(Tr(td(['',$image_count]))) : '') . br;
  }
  my $title = "Release $release";
  my $t = param('title');
  if ($t) {
    $title .= " ($t)";
    $title = span({style => "background: $BG{$t}"},$title);
  }
  print h1($title),$display,
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
  # Render
  if ($waiting) {
    print div({class => 'panel panel-info'},
              div({class => 'panel-heading'},
                  span({class => 'panel-heading;'},
                       'Awaiting publishing')),
              div({class => 'panel-body'},$waiting))
          . div({style => 'clear: both;'},NBSP);
  }
  my $render = '';
  my $h = sprintf '%dpx',300 + 30 * $release_count;
  foreach ('Pre-staged','Staged','Production') {
    $render .= div({class => 'boxed',
                    style => "float: left; width: 350px; height: $h;background-color: $BG{$_};"},
                   h1({style => 'color: #fff'},$_),$published{$_});
  }
  print div({class => 'panel panel-success'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},'Published')),
            div({class => 'panel-body'},
                div({style => 'float: left;'},$render)));
  print end_form,&sessionFooter($Session),end_html;
}


sub getPrestagedData
{
  my $service = JFRC::LDAP->new();
  my ($annotator,$ar,$waiting) = ('')x3;
  # Waiting
  $sths{WAITING}->execute();
  $ar = $sths{WAITING}->fetchall_arrayref();
  if (scalar @$ar) {
    my (%hist,%line);
    my $images = 0;
    foreach (@$ar) {
      $line{$_->[0]}++;
      unless ($_->[1]) {
        $_->[1] = 'FLEW';
        $_->[2] = '(FLEW)';
      }
      if ($_->[3]) {
        my $u = $service->getUser($_->[3]);
        $annotator = join(' ',$u->{givenName},$u->{sn});
      }
      $_->[3] = ($annotator ne ' ') ? $annotator : $_->[3];
      my @o;
      foreach my $o (split(', ',$_->[5])) {
        $o =~ s/\D+(\d+[Xx]).+/$1/;
        push @o,$o;
      }
      $_->[5] = join(', ',@o);
      $hist{$_->[-1]}++;
      $images += $_->[-1];
    }
    $waiting = table({id => 'waiting',class => 'tablesorter standard'},
                     thead(Tr(th(['Line','Website','ALPS release','Annotator',
                                  'Tiles','Objectives','Reporters','Images']))),
                     tbody(map {Tr(td($_))} @$ar),
                     tfoot(Tr(td([scalar keys(%line),('')x6,$images]))));
    $waiting .= h3('Number of images per line')
                . table({id => 'ipl',class => 'tablesorter standard'},
                        thead(Tr(th(['Lines','Images']))),
                        tbody(map {Tr(td($_),td($hist{$_}))} sort keys %hist));
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
        $_->[1] = a({href => "?release=$_->[1]&title=Pre-staged",
                     target => '_blank'},$_->[1]);
      }
    }
    $sths{SPLIT_GAL4}->execute();
    foreach (keys %group) {
      $group{$_}{lines} = $sths{SPLIT_GAL4}->fetchrow_array() if (/Split GAL4/);
    }
    $published = table({id => 'published',class => 'tablesorter standard'},
                       thead(Tr(th(['Website','ALPS release','Lines','Images']))),
                       tbody(map {Tr(td($_))} @$ar),
                       tfoot(Tr(td(['','',$line_count,$image_count]))))
                 . table({class => 'standard'},
                         thead(Tr(th(['Website','Lines','Images']))),
                         tbody(map {Tr(td([$_,$group{$_}{lines},$group{$_}{images}]))} sort {$group{$a}{name} cmp $group{$b}{name}} keys %group));
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
      $_->[1] = span({style => 'color: red'},$_->[1])
        if ($line_check{$previous}{$_->[0]} != $_->[1]);
      $_->[2] = span({style => 'color: red'},$_->[2])
        if ($image_check{$previous}{$_->[0]} != $_->[2]);
      $_->[0] = a({href => "?release=$_->[0]&instance=$instance&title=$current",
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
    $published .= table({id => 'publishedh',class => 'tablesorter standard'},
                        thead(Tr(th(['Lines','Images']))),
                        tbody(map {Tr(td($_))} @$ar),
                        tfoot(Tr(td(['',$tsh]))));
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
                    ('highcharts-4.0.1/highcharts',
                     'highcharts-4.0.1/highcharts-more',
                     'jquery/jquery.tablesorter','tablesorter');
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
