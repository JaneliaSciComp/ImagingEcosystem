#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use CGI::Carp qw(fatalsToBrowser);
use DBI;
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
our $APPLICATION = 'Janelia Workstation sample search';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Database
my %sth = (
SAMPLES => "SELECT name FROM entity WHERE entity_type='Sample' AND name "
           . "NOT LIKE '%~%' ORDER BY 1",
SLIDES => "SELECT DISTINCT value FROM entityData WHERE "
          . "entity_att='slide code' ORDER BY 1",
LINES => "SELECT DISTINCT value FROM entityData WHERE entity_att='line' "
         . "ORDER BY 1",
SAMPLESUM => "SELECT name FROM entity WHERE entity_type='Sample' AND "
             . "name LIKE ? AND name NOT LIKE '%~%' ORDER BY 1",
SLIDESUM => "SELECT DISTINCT e.name FROM entity e JOIN entityData ed ON "
            . "(e.id=ed.parent_entity_id AND ed.entity_att='Slide Code') "
            . "WHERE ed.value LIKE ? AND e.entity_type='Sample'",
LINESUM => "SELECT DISTINCT e.name FROM entity e JOIN entityData ed ON "
           . "(e.id=ed.parent_entity_id AND ed.entity_att='Line') "
           . "WHERE ed.value LIKE ? AND e.entity_type='Sample'",
SAMPLE => "SELECT te.event_type,t.job_name,te.description,te.event_timestamp FROM "
          . "task_event te JOIN task_parameter tp ON (tp.task_id=te.task_id "
          . "AND parameter_name='sample entity id') JOIN task t ON "
          . "(t.task_id=te.task_id) WHERE tp.parameter_value=? ORDER BY 4",
ED => "SELECT parent_entity_id,entity_att,value FROM entityData ed "
      . "JOIN entity e ON (e.id=ed.parent_entity_id AND entity_type='Sample') "
      . "WHERE e.name=? ORDER BY 1,2",
EED => "SELECT ed.id,e.name,e.entity_type,ed.entity_att,ed.value,ed.child_entity_id,e1.entity_type FROM entity e "
       . "LEFT OUTER JOIN entityData ed ON (e.id=ed.parent_entity_id) LEFT OUTER JOIN entity e1 ON (e1.id=ed.child_entity_id) "
       . "WHERE e.id=? ORDER BY 4",
# ----------------------------------------------------------------------
SAGE_LSMS => "SELECT family,name,objective,area,tile FROM image_data_mv WHERE "
             . "slide_code=? AND line=? ORDER BY 1",
SAGE_CT => "SELECT DATEDIFF(?,MAX(create_date)) FROM image WHERE id IN "
           . "(SELECT id FROM image_data_mv WHERE slide_code=? AND line=?)",
# ----------------------------------------------------------------------
FB_CT2 => "SELECT DATEDIFF(?,MAX(event_date)) FROM stock_event_history_vw "
          . "WHERE cross_barcode=?",
);
my @TAB_ORDER = qw(line slide sample);

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
my $SCICOMP = ($Session->param('scicomp'));

our ($dbh,$dbhf,$dbhs);
# Connect to databases
&dbConnect(\$dbh,'workstation')
  || &terminateProgram("Could not connect to Fly Portal: ".$DBI::errstr);
&dbConnect(\$dbhf,'flyboy')
  || &terminateProgram("Could not connect to FlyBoy: ".$DBI::errstr);
&dbConnect(\$dbhs,'sage')
  || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
foreach (keys %sth) {
  if (/^SAGE_/) {
    (my $n = $_) =~ s/SAGE_//;
    $sth{$n} = $dbhs->prepare($sth{$_}) || &terminateProgram($dbhs->errstr);
  }
  elsif (/^FB_/) {
    (my $n = $_) =~ s/FB_//;
    $sth{$n} = $dbhf->prepare($sth{$_}) || &terminateProgram($dbhf->errstr);
  }
  else {
    $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
  }

}

&showOutput();

# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************


sub showOutput
{
  # ----- Page header -----
  print &pageHead(),start_form;
  if (param('sample_id')) {
    print &getSample('sample',param('sample_id'));
  }
  elsif (param('entity_id')) {
    print &getEntity(param('entity_id'),0);
  }
  else {
    print &showQuery();
  }
  # ----- Footer -----
  print div({style => 'clear: both;'},NBSP),end_form,
        &sessionFooter($Session),end_html;
}


sub showQuery {
  my $html = '';
  my $ar;
  my %tab = (sample => {active => '',
                        title => 'Search by Sample ID',
                        populatex => 'SAMPLES'},
             slide => {active => '',
                       title => 'Search by slide code',
                       populate => 'SLIDES'},
             line => {active => '',
                      title => 'Search by line',
                      populate => 'LINES'},
            );
  my $found = 0;
  # Set active search
  my $active_search = '';
  foreach (@TAB_ORDER) {
    $active_search = $_ if param($_ . '_search');
  }
  # Set active tab
  foreach (@TAB_ORDER) {
    if (param($_ . '_active') || ($active_search eq $_)) {
      $tab{$_}{active} = 'active';
      $found++;
    }
  }
  $tab{line}{active} = 'active' unless ($found);
  foreach (@TAB_ORDER) {
    next if ($active_search && !param($_ . '_search'));
    if (exists $tab{$_}{populate} && !param($_ . '_search')) {
      $sth{$tab{$_}{populate}}->execute();
      $ar = $sth{$tab{$_}{populate}}->fetchall_arrayref();
      $tab{$_}{content} = ucfirst($_) . ': '
        . popup_menu(&identify($_.'_id'),
                     'data-placeholder' => "Choose a $_..",
                     -class => 'chosen-select',
                     -values => ['',map {$_->[0]} @$ar]);
    }
    else {
      $tab{$_}{content} = "Enter a " . ucfirst($_)
                          . ' ID (or a portion of one): '
                          . input({&identify($_.'_idi'),
                                   value => param($_.'_id')||''})
    }
    $tab{$_}{content} .= br
                         div({align => 'center'},
                             submit({&identify($_.'_search'),
                                     class => 'btn btn-success',
                                     value => 'Search'}));
    if (param($_ . '_search')) {
      my $cur = uc($_) . 'SUM';
      my($term) = param($_ . '_idi')
                  ? '%' . param($_ . '_idi') . '%'
                  : param($_ . '_id');
      $sth{$cur}->execute($term);
      $ar = $sth{$cur}->fetchall_arrayref();
      if (scalar @$ar) {
        my $t = $_;
        $tab{$_}{content} .= br
                             . 'Number of records: ' . scalar(@$ar)
                             . table({class => 'tablesorter standard'},
                                     thead(Tr(th('Sample'))),
                                     tbody(map {Tr(td(a({href => "?sample_id=".$_->[0]},$_->[0])))} @$ar));
      }
      else {
        $tab{$_}{content} .= br
                             . &bootstrapPanel('Not found',ucfirst(lc($_))
                                               . " IDs containing "
                                               . param($_ . '_id')
                                               . " were not found",'danger');
      }
    }
  }
  $html .= div({role => 'tabpanel'},
               ul({class => 'nav nav-tabs',role => 'tablist'},
                  map {li({role => 'presentation',class => $tab{$_}{active}},
                          a({href => '#'.$_,role => 'tab',
                             'data-toggle' => 'tab'},$tab{$_}{title}))
                      } @TAB_ORDER),
               div({class => 'tab-content'},
                   map {div({role => 'tabpanel',id => $_,
                             class => 'tab-pane ' . $tab{$_}{active}},
                            br . $tab{$_}{content})
                       } @TAB_ORDER
                  ));
  $html = div({class => 'boxed'},$html);
}


sub getEntity
{
  my($id,$skip_header) = @_;
  $sth{EED}->execute($id);
  my $ar = $sth{EED}->fetchall_arrayref();
  # Entity ID, name, entity type, entity attribute, value, child entity ID
  my ($name,$type) = ('')x2;
  my %att;
  my $html = '';
  foreach (@$ar) {
    my $cet = pop @$_;
    unless ($name) {
      ($name,$type) = ($_->[1],$_->[2]);
      $html = h2($type . ": $name") . br unless ($skip_header);
    }
    if ($_->[3] eq 'Line') {
      $_->[4] = a({href => "lineman.cgi?line=".$_->[4],
                   target => '_blank'},$_->[4]);
    }
    elsif ($_->[3] eq 'Slide Code') {
      $_->[4] = a({href => "/slide_search.php?term=slide_code&id=".$_->[4],
                   target => '_blank'},$_->[4]);
    }
    elsif ($_->[3] eq 'Entity' && length($cet)) {
      $_->[3] = "Entity ($cet)";
    }
    elsif ($SCICOMP && ($_->[4] =~ /\.png$/)) {
      (my $i = $_->[4]) =~ s/.+filestore\///;
      $i = "/imagery_links/ws_imagery/$i";
      $_->[4] .= NBSP
                 . a({href => "http://jacs-webdav.int.janelia.org/WebDAV$_->[4]",
                      target => '_blank'},
                     img({src => $i,
                          width => '10%'}));
    }
    # EID -> [attribute, value, child EID]
    $att{$_->[0]} = [$_->[3],$_->[4],$_->[5]];
  }
  if (scalar keys %att > 1) {
    my $msg = h6('Attributes that are links may be followed to look at the child entity for that attribute.');
    my $t = table({class => 'tablesorter standard'},
                  thead(Tr(th([qw(Attribute Value)]))),
                  tbody(map {my $l = ($att{$_}[2])
                                     ? a({href => "?entity_id=".$att{$_}[2],
                                          target => '_blank'},$att{$_}[0])
                                     : $att{$_}[0];
                             Tr(td([$l,$att{$_}[1]]))
                            } sort {$att{$a}[0] cmp $att{$b}[0]} keys %att));
    $html .= ($skip_header) ? ($t . $msg)
                            : &bootstrapPanel("Entity ID $id",$t.$msg,'standard');
  }
  else {
    my $msg = h4('This entity has no attributes');
    $html .= ($skip_header) ? $msg
                            : &bootstrapPanel("Entity ID $id",$msg,'standard');
  }
  return($html);
}


sub getSample
{
  my($type,$id) = @_;
  my $html = h2(ucfirst($type) . " $id") . br;
  $sth{ED}->execute($id);
  my $ar = $sth{ED}->fetchall_arrayref();
  my %sample;
  $sample{$_->[0]}{$_->[1]} = $_->[2] foreach (@$ar);
  if (scalar(keys %sample) > 1) {
    my $msg = 'Found ' . scalar(keys %sample) .  " entities for $id" . br
              . 'Entities are shown most-recent first. Entities in '
              . '"Desync" status are maked with a red header.';
    $html .= &bootstrapPanel('Multiple entities',$msg,'warning');
  }
  foreach my $eid (sort {$b <=> $a} keys %sample) {
    $sth{uc($type)}->execute($eid);
    my $ar2 = $sth{uc($type)}->fetchall_arrayref();
    my $tasks = '';
    if (scalar @$ar2) { 
      foreach (@$ar2) {
        $_->[1] = a({href => "/flow_ws.php?flow=$_->[1]",
                     target => '_blank'},$_->[1]) if ($_->[1]);
      }
      $tasks = h3('Task events')
               . table({class => 'tablesorter standard'},
                       thead(Tr(th([qw(Event Job Description Date)]))),
                       tbody(map {Tr(td($_))} @$ar2));
      my($last_event,$last_time) = ($ar2->[-1][0],$ar2->[-1][3]);
      my($cross,$line,$slide) = ('')x2;
      foreach $a (keys %sample) {
        $cross = $sample{$a}{'Cross Barcode'} if (exists $sample{$a}{'Slide Code'});
        $slide = $sample{$a}{'Slide Code'} if (exists $sample{$a}{'Slide Code'});
        $line = $sample{$a}{Line} if (exists $sample{$a}{Line});
      }
      if ($line && $slide) {
        $sth{LSMS}->execute($slide,$line);
        $ar = $sth{LSMS}->fetchall_arrayref();
        $tasks .= h3('Associated LSM files')
                  . table({class => 'tablesorter standard'},
                          thead(Tr(td(['Name','Objective','Area','Tile']))),
                          tbody(map {$a = a({href => "view_sage_imagery.cgi?_op=stack;_family=$_->[0];_image=$_->[1]",
                                             target => '_blank'},$_->[1]);
                                     Tr(td([$a,@$_[2,3,4]]) )
                                    } @$ar)) if (scalar @$ar);
        if ($last_event eq 'completed' && $line && $slide) {
          my $ctm;
          $sth{CT}->execute($last_time,$slide,$line);
          my($ct) = $sth{CT}->fetchrow_array();
          if ($ct) {
            my $c = sprintf 'tmog &rarr; Image processing completion cycle time: %d day%s',$ct,(1 == $ct) ? '' : 's';
            $ctm .= p({class => ($ct < 0) ? 'bg-danger' : 'bg-primary'},$c);
          }
          $sth{CT2}->execute($last_time,$cross);
          ($ct) = $sth{CT2}->fetchrow_array();
          if ($ct) {
            my $c = sprintf 'Cross &rarr; Image processing completion cycle time: %d day%s',$ct,(1 == $ct) ? '' : 's';
            $ctm .= p({class => ($ct < 0) ? 'bg-danger' : 'bg-primary'},$c);
          }
          $tasks .= h3('Cycle time') . $ctm if ($ctm);
        }
      }
    }
    my $class = ($sample{$eid}{Status} eq 'Desync')
                ? 'danger' : 'primary';
    $html .= &bootstrapPanel("Entity ID $eid",&getEntity($eid,1).$tasks,$class);
  }
  return($html);
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
