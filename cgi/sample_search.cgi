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
SAMPLE => "SELECT t.event_type,t.description,t.event_timestamp FROM "
          . "task_event t JOIN task_parameter tp ON (tp.task_id=t.task_id "
          . "AND parameter_name='sample entity id') "
          . "WHERE tp.parameter_value=? ORDER BY 3",
ED => "SELECT parent_entity_id,entity_att,value FROM entityData ed "
      . "JOIN entity e ON (e.id=ed.parent_entity_id AND entity_type='Sample') "
      . "WHERE e.name=? ORDER BY 1,2",
);
my @TAB_ORDER = qw(line slide sample);

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
my $SCICOMP = ($Session->param('scicomp'));

our ($dbh);
# Connect to databases
&dbConnect(\$dbh,'workstation')
  || &terminateProgram("Could not connect to Fly Portal: ".$DBI::errstr);
$sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  foreach (keys %sth);

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
    print &getRecord('sample',param('sample_id'));
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


sub getRecord
{
  my($type,$id) = @_;
  my $html = h2(ucfirst($type) . " $id") . br;
  $sth{ED}->execute($id);
  my $ar = $sth{ED}->fetchall_arrayref();
  my %sample;
  foreach (@$ar) {
    if ($_->[1] eq 'Line') {
      $_->[2] = a({href => "lineman.cgi?line=".$_->[2],
                   target => '_blank'},$_->[2]);
    }
    elsif ($_->[1] eq 'Slide Code') {
      $_->[2] = a({href => "/slide_search.php?term=slide_code&id=".$_->[2],
                   target => '_blank'},$_->[2]);
    }
    elsif ($SCICOMP && ($_->[2] =~ /\.png$/)) {
      (my $i = $_->[2]) =~ s/.+filestore\///;
      $i = "/imagery_links/ws_imagery/$i";
      $_->[2] .= NBSP . img({src => $i,
                             width => '10%'});
    }
    $sample{$_->[0]}{$_->[1]} = $_->[2];
  }
  $html .= &bootstrapPanel('Multiple entities',
                           'Found ' . scalar(keys %sample)
                           . " entities for $id",'warning')
    if (scalar(keys %sample) > 1);
  foreach my $eid (sort keys %sample) {
    $sth{uc($type)}->execute($eid);
    my $ar2 = $sth{uc($type)}->fetchall_arrayref();
    my $tasks = '';
    if (scalar @$ar2) { 
      $tasks = table({class => 'tablesorter standard'},
                     thead(Tr(th([qw(Event Description Date)]))),
                     tbody(map {Tr(td($_))} @$ar2));
    }
    $html .= div({class => 'boxed'},
                 h3("Entity ID $eid"),br,
                 table({class => 'tablesorter standard'},
                        thead(Tr(th([qw(Attribute Value)]))),
                        tbody(map {Tr(td([$_,$sample{$eid}{$_}]))} sort keys %{$sample{$eid}})),
                 br,$tasks) . br;
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
