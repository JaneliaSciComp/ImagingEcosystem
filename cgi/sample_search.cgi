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
use MongoDB;
use POSIX qw(strftime);
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/groups/scicompsoft/informatics/data/';
use constant NBSP => '&nbsp;';
my @CLOSED = ('right','display:none;');
my @OPEN = ('down','display:block;');
my $AUTHORIZED = 0;
my $ID = 0;

# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Janelia Workstation sample search';
my $BASE = "/var/www/html/output/";
my (%CONFIG,%SERVER);
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
my $MONGO = 1;
my $SELECTOR = "SELECT DISTINCT e.name,edl.value,eds.value,ede.value,edd.value,edi.value FROM entity e JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Slide Code') JOIN entityData edl ON (e.id=edl.parent_entity_id AND edl.entity_att='Line') JOIN entityData edd ON (e.id=edd.parent_entity_id AND edd.entity_att='Data Set Identifier') LEFT OUTER JOIN entityData ede ON (e.id=ede.parent_entity_id AND ede.entity_att='Effector') LEFT OUTER JOIN entityData edi ON (e.id=edi.parent_entity_id AND edi.entity_att='Default 2D Image') ";
my %sth = (
SAMPLES => "SELECT name FROM entity WHERE entity_type='Sample' AND name "
           . "NOT LIKE '%~%' ORDER BY 1",
SLIDES => "SELECT DISTINCT value FROM entityData WHERE "
          . "entity_att='slide code' ORDER BY 1",
LINES => "SELECT DISTINCT value FROM entityData WHERE entity_att='line' "
         . "ORDER BY 1",
DATASETS => "SELECT DISTINCT value FROM entityData WHERE entity_att='Data Set Identifier' "
           . "ORDER BY 1",
SAMPLESUM => "$SELECTOR WHERE e.name LIKE ? AND e.entity_type='Sample' ORDER BY 1",
SLIDESUM => "$SELECTOR WHERE eds.value LIKE ? AND e.entity_type='Sample' ORDER BY 1",
LINESUM => "$SELECTOR WHERE edl.value LIKE ? AND e.entity_type='Sample' ORDER BY 1",
DATASETSUM => "$SELECTOR WHERE edd.value LIKE ? AND e.entity_type='Sample' ORDER BY 1",
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
CED => "SELECT value FROM entityData WHERE parent_entity_id=? AND entity_att=?",
# ----------------------------------------------------------------------
SAGE_LSMS => "SELECT family,i.name,objective,area,tile,url FROM "
             . "image_data_mv id JOIN image i ON (i.id=id.id) "
             . "WHERE slide_code=? AND line=? ORDER BY 1",
SAGE_CT => "SELECT DATEDIFF(?,MAX(create_date)) FROM image WHERE id IN "
           . "(SELECT id FROM image_data_mv WHERE slide_code=? AND line=?)",
# ----------------------------------------------------------------------
FB_CT2 => "SELECT DATEDIFF(?,MAX(event_date)) FROM stock_event_history_vw "
          . "WHERE cross_barcode=?",
);
my %GROUP;
my @TAB_ORDER = qw(line slide dataset sample);

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
our $USERID = $Session->param('user_id');
our $USERNAME = $Session->param('user_name');
$AUTHORIZED = 1 if (($Session->param('scicomp'))
                    || ($Session->param('workstation_flylight')));
my $WIDTH = param('width') || 150;

our ($dbh,$dbhf,$dbhs);
my $Subject;
# Database config
my $file = DATA_PATH . 'db_config.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
my $hr = decode_json $slurp;
%CONFIG = %$hr;
my $mongodb = MongoDB->connect($CONFIG{uri});
$Subject= $mongodb->ns('jacs.subject');
# REST services
$file = DATA_PATH . 'rest_services.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
my $hr = decode_json $slurp;
%CONFIG = %$hr;
# Servers
$file = DATA_PATH . 'servers.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
$hr = decode_json $slurp;
%SERVER = %$hr;
# Connect to databases
unless ($MONGO) {
  &dbConnect(\$dbh,'workstation')
    || &terminateProgram("Could not connect to Fly Portal: ".$DBI::errstr);
}
&dbConnect(\$dbhf,'flyboy')
  || &terminateProgram("Could not connect to FlyBoy: ".$DBI::errstr);
&dbConnect(\$dbhs,'sage')
  || &terminateProgram("Could not connect to SAGE: ".$DBI::errstr);
foreach (@TAB_ORDER) {
  $sth{uc($_).'SUM'} =~ s/ LIKE /=/ if ((param($_.'_match')) && (param($_.'_match') =~ /exact/));
}
foreach (keys %sth) {
  if (/^SAGE_/) {
    (my $n = $_) =~ s/SAGE_//;
    $sth{$n} = $dbhs->prepare($sth{$_}) || &terminateProgram($dbhs->errstr);
  }
  elsif (/^FB_/) {
    (my $n = $_) =~ s/FB_//;
    $sth{$n} = $dbhf->prepare($sth{$_}) || &terminateProgram($dbhf->errstr);
  }
  elsif (!$MONGO) {
    $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr);
  }
}

&showOutput();

# We're done!
if ($dbh && !$MONGO) {
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
    if ($MONGO) {
      print &getSampleJSON(param('sample_id'));
    }
    else {
      print &getSample('sample',param('sample_id'));
    }
  }
  elsif (param('entity_id')) {
    print &getEntity(param('entity_id'),0);
  }
  else {
    print &showQuery();
  }
  # ----- Footer -----
  print div({style => 'clear: both;'},NBSP),
        hidden(&identify('mongo'),default=>param('mongo')),
        end_form,&sessionFooter($Session),end_html;
}


sub showQuery {
  my $html = '';
  my $ar;
  my %tab = (sample => {active => '',
                        title => 'Search by Sample ID'},
             slide => {active => '',
                       title => 'Search by slide code'},
             line => {active => 1,
                      title => 'Search by line'},
             dataset => {active => '',
                         title => 'Search by data set',
                         populate => 'DATASETS'},
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
    if (exists $tab{$_}{populate} && $tab{$_}{populate}) {
      my $rvar;
      if ($MONGO) {
        ($rvar) = &getMONGO($tab{$_}{populate});
        $ar = [];
        if ($_ eq 'dataset') {
          push @$ar,[$_->{identifier}]
            foreach (sort {$a->{identifier} cmp $b->{identifier}} @$rvar);
        }
        else {
          push @$ar,[$_] foreach (sort @$rvar);
        }
      }
      else {
        $sth{$tab{$_}{populate}}->execute();
        $ar = $sth{$tab{$_}{populate}}->fetchall_arrayref();
      }
      $tab{$_}{content} = ucfirst($_) . ': '
        . popup_menu(&identify($_.'_id'),
                     'data-placeholder' => "Choose a $_..",
                     -class => 'chosen-select',
                     -values => ['',map {$_->[0]} @$ar]);
    }
    else {
      $tab{$_}{content} = "Search for a "
                          . popup_menu(&identify($_.'_match'),
                                       -values => ['portion of','exact match for'])
                          . " a " . ucfirst($_)
                          . ' ID: '
                          . input({&identify($_.'_idi'),
                                   value => param($_.'_id')||param($_.'_idi')||''})
    }
    if ($AUTHORIZED) {
      $tab{$_}{content} .= br
                           . checkbox(&identify($_.'_display'),
                                      -label => ' Display imagery',
                                      -checked => 0)
                           . (NBSP)x5 . 'Width: '
                           . input({&identify($_.'_width'),
                                    size => '3em',
                                    value => param($_.'_width')||$WIDTH});
    }
    $tab{$_}{content} .= br
                         div({align => 'center'},
                             submit({&identify($_.'_search'),
                                     class => 'btn btn-success',
                                     value => 'Search'}));
    if (param($_ . '_search')) {
      my $DISPLAY = param($_ . '_display') || '';
      $DISPLAY = '' unless ($AUTHORIZED);
      $WIDTH = param($_ . '_width') || $WIDTH;
      my $cur = uc($_) . 'SUM';
      my $term;
      if (param($_ . '_idi')) {
        $term = (param($_ . '_match') =~ /portion/)
          ? '%' . param($_ . '_idi') . '%'
          : param($_ . '_idi');
      }
      else {
        $term = param($_ . '_id');
      }
      my $rvar;
      if ($MONGO) {
        ($rvar) = &getMONGO($cur,$term);
        $ar = [];
        foreach (sort @$rvar) {
          push @$ar,[@{$_}{qw(name line slideCode effector dataSet status defaultImage)}];
        }
      }
      else {
        $sth{$cur}->execute($term);
        $ar = $sth{$cur}->fetchall_arrayref();
      }
      if (scalar @$ar) {
        my $t = $_;
        my @row;
        my @header = ('Sample','Line','Slide code','Effector','Data set','Status');
        push @header,'Default image' if ($AUTHORIZED);
        foreach my $r (@$ar) {
          $r->[5] = &renderStatus($r->[5]);
          if ($AUTHORIZED) {
            if ($r->[-1]) {
              $r->[-1] = a({href => "$SERVER{'jacs-storage'}{address}$r->[-1]",
                           target => '_blank'},
                          img({src => "$SERVER{'jacs-storage'}{address}$r->[-1]",
                               width => $WIDTH}));
            }
            else {
              $r->[-1] = '(no image found)';
            }
          }
          else {
            pop @$r;
          }
          my $addr = "?sample_id=" . $r->[0];
          $addr .= ";mongo=1" if ($MONGO);
          if ($DISPLAY) {
            push @row,div({class => 'sample'},$r->[-1],br,
                          a({href => $addr},$r->[0]));
          }
          else {
            push @row,[a({href => $addr},$r->[0]),
                       @{$r}[1..$#$r]];
          }
        }
        $tab{$_}{content} .= div({style => 'clear: both;'},NBSP)
                             . 'Number of records: ' . scalar(@$ar)
                             . (NBSP)x5
                             . &createExportFile($ar,'_sample_search',\@header)
                             . br;
        if ($DISPLAY) {
          $tab{$_}{content} .= div({class => 'sample_container'},@row);
        }
        else {
          $tab{$_}{content} .= table({class => 'tablesorter standard'},
                                     thead(Tr(th(\@header))),
                                     tbody(map {Tr(td($_))} @row));
        }
      }
      else {
        $tab{$_}{content} .= br
                             . &bootstrapPanel('Not found',ucfirst(lc($_))
                                               . " IDs containing "
                                               . param($_ . '_idi')
                                               . " were not found",'danger');
      }
      $tab{$_}{content} .= br;
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


sub getMONGO
{
  my($att,$value) = @_;
  my($selector,$suffix) = ('')x2;
  if ($att eq 'DATASETS') {
    $selector = 'SageImagery';
  }
  elsif ($att eq 'LINES') {
    $selector = 'SampleAttribute';
    $suffix = '?attribute=line';
  }
  elsif ($att eq 'SLIDES') {
    $selector = 'SampleAttribute';
    $suffix = '?attribute=slideCode';
  }
  elsif ($att eq 'DATASETSUM') {
    $selector = 'SampleSearch';
    $suffix = "?dataSet=$value";
  }
  elsif ($att eq 'LINESUM') {
    $selector = 'SampleSearch';
    $suffix = "?line=$value";
  }
  elsif ($att eq 'SAMPLESUM') {
    $selector = 'SampleSearch';
    $suffix = "?name=$value";
  }
  elsif ($att eq 'SLIDESUM') {
    $selector = 'SampleSearch';
    $suffix = "?slideCode=$value";
  }
  elsif ($att eq 'SAMPLE') {
    $selector = 'SampleJSON';
    $suffix = ($value =~ /^\d+$/) ? "?sampleId=$value" : "?name=$value";
    # $suffix = "?name=$value";
  }
  elsif ($att eq 'EVENTS') {
    $selector = 'SampleEvents';
    $suffix = "?sampleId=$value";
  }
  if ($suffix =~ /%/) {
    $suffix =~ s/%//g;
    $suffix .= '&wildcard=true';
  }
  my $rest = $CONFIG{'jacs'}{url}.$CONFIG{'jacs'}{query}{$selector} . $suffix;
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


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d_%H:%M:%S",localtime)
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
      if ($cet eq 'Pipeline Run' && $_->[5]) {
        $sth{CED}->execute($_->[5],'Pipeline Process');
        my($v) = $sth{CED}->fetchrow_array();
        $_->[4] = a({href => "/flow_ws.php?dataset=PipelineConfig_$v",
                     target => '_blank'},$v) if ($v);
      }
    }
    elsif ($_->[3] eq 'Pipeline Process') {
      $_->[4] = a({href => "/flow_ws.php?dataset=PipelineConfig_$_->[4]",
                   target => '_blank'},$_->[4]) if ($_->[4]);
    }
    elsif ($AUTHORIZED && ($_->[4] =~ /\.(?:mp4|png)$/)) {
      $_->[4] .= NBSP
                 . a({href => "$SERVER{'jacs-storage'}{address}$_->[4]",
                      target => '_blank'},
                     img({src => "$SERVER{'jacs-storage'}{address}$_->[4]",
                          width => $WIDTH}));
    }
    # EID -> [attribute, value, child EID]
    $att{$_->[0]} = [$_->[3],$_->[4],$_->[5]];
  }
  if (scalar keys %att) {
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


sub getSampleJSON
{
  my($name) = @_;
  my $html = hr((($MONGO) ? img({src => '/images/mongodb.png'}) : ''),
                "Sample $name") . br;
  my $samples = &getMONGO('SAMPLE',$name);
  if (scalar(@$samples) > 1) {
    my $msg = 'Found ' . scalar(@$samples) .  " samples for $name";
    $html .= &bootstrapPanel('Multiple samples',$msg,'warning');
  }
  foreach my $s (@$samples) {
    my $sid = $s->{_id};
    my $task = &getMONGO('EVENTS',$sid);
    my @td;
    $AUTHORIZED = &getAuthorization($s);
    my($arrow,$display) = @CLOSED;
    foreach (sort keys %{$s}) {
      my $r = ref($s->{$_});
      if ($r eq 'ARRAY') {
        if (ref($s->{$_}[0])) {
          my $content;
          foreach my $o (@{$s->{$_}}) {
            $content .= h2($o->{objective} . ' objective');
            if ($a = scalar(@{$o->{tiles}})) {
              $content .= h3("Tiles: $a");
              foreach my $t (@{$o->{tiles}}) {
                my $tid = join('_',$o->{objective},$t->{anatomicalArea},$t->{name});
                my $n = delete($t->{anatomicalArea}) . ' (' . delete($t->{name}) . ')';
                my $tcontent = &process_lsmReferences($t,@CLOSED);
                $content .= div({style => 'margin-left: 15px;'},
                                h4(&toggle($tid,$arrow),$n),
                                div({&identify($tid),
                                     style => $display},$tcontent));
              }
            }
            if ($a = scalar(@{$o->{pipelineRuns}})) {
              $content .= h3("Pipeline runs: $a");
              foreach my $p (@{$o->{pipelineRuns}}) {
                my $n = delete $p->{name};
                $n .= ' ' . delete $p->{creationDate};
                my $id = join('_','pipeline',$p->{id});
                my $pcontent = '';
                if ($a = scalar(@{$p->{results}})) {
                  $pcontent = h3("Results: $a");
                  foreach my $r (@{$p->{results}}) {
                    my $n = delete $r->{name};
                    my $id = join('_','result',$ID++);
                    $pcontent .= div({style => 'margin-left: 15px;'},
                                     h4(&toggle($id,$arrow),$n),
                                     div({&identify($id),
                                          style => 'margin-left: 15px;'.$display},pre(to_json($r,{utf8 => 1, pretty => 1}))));
                  }
                  delete $p->{results};
                }
                $pcontent .= div({style => 'margin-left: 15px;'},pre(to_json($p,{utf8 => 1, pretty => 1})));
                $content .= div({style => 'margin-left: 15px;'},
                                h4(&toggle($id,$arrow),$n),
                                div({&identify($id),
                                     style => $display},$pcontent));
              }
            }
          }
          push @td,[$_,$content];
        }
        else {
          push @td,[$_,join(', ',sort @{$s->{$_}})];
        }
      }
      else {
        if (($_ eq '_id') && $AUTHORIZED) {
          $s->{$_} .= a({href => "image_secdata.cgi?id=$s->{$_}",
                         target => '_blank'},' (View secondary data)');
        }
        elsif ($_ eq 'status') {
          $s->{$_} = &renderStatus($s->{$_});
        }
        push @td,[$_,$s->{$_}];
      }
    }
    $html .= table({class => 'tablesorter standard'},
                   thead(Tr(th([qw(Key Value)]))),
                   tbody(map {Tr(td($_))} @td));
    if (scalar @$task) {
      my $id = '';
      my $alt = 1;
      $html .= h3('Task events')
               . table({class => 'standard'},
                       thead(Tr(th([qw(Event Job Description Date),'Task ID']))),
                       tbody(map {if ($_->{taskId} != $id) {
                                    $alt = 1 - $alt;
                                    $id = $_->{taskId};
                                  }
                                  Tr({class => "row_alt$alt"},
                                     td([$_->{eventType},
                                         a({href => "/flow_ws.php?flow=$_->{jobName}",
                                            target => '_blank'},$_->{jobName}),
                                         ($_->{description}||''),$_->{timestamp},$_->{taskId}]))}
                                 @$task));
    }
  }
  return($html);
}


sub renderStatus
{
  my $status = shift;
  my $style = 'font-weight: bold; color: ';
  if ($status eq 'Complete') {
    $status = span({style => $style . 'green'},$status);
  }
  elsif ($status eq 'Error') {
    $status = span({style => $style . 'red'},$status);
  }
  elsif ($status eq 'Processing') {
    $status = span({style => $style . 'blue'},$status);
  }
  elsif ($status =~ /^(?:Desync|Marked)/) {
    $status = span({style => $style . 'orange'},$status);
  }
  return($status);
}


sub getAuthorization
{
  my($s) = shift;
  return(1) if (($Session->param('scicomp'))
                || ($Session->param('workstation_flylight')));
  my %authorized;
  &addAuthorizedUser(split(':',$s->{ownerKey}),\%authorized);
  foreach (@{$s->{readers}}) {
    &addAuthorizedUser(split(':',$_),\%authorized);
  }
  return(exists $authorized{$USERID});
}


sub addAuthorizedUser
{
  my($type,$name,$hr) = @_;
  if ($type eq 'user') {
    $$hr{$name}++;
    return();
  }
  elsif (!exists($GROUP{$name})) {
    my $data = $Subject->find({groups => {'$in' => ["group:$name"]}}, {_id => 0, key =>1});
    while (my $row = $data->next) {
      push @{$GROUP{$name}},(split(':',$row->{key}))[1];
    }
  }
  $hr->{$_}++ foreach (@{$GROUP{$name}});
}


sub process_lsmReferences
{
  my($t,$arrow,$display) = @_;
  my $tcontent = '';
  if (scalar @{$t->{lsmReferences}}) {
    foreach my $l (@{$t->{lsmReferences}}) {
      my ($fcontent,$strip) = ('')x2;
      my $n = delete $l->{name};
      my $lid = join('_',$l->{'_id'});
      if (exists $l->{files}) {
        my @additional;
        foreach my $f (sort keys %{$l->{files}}) {
          if ($l->{files}{$f} =~ /\.png$/) {
            $strip .= div({style => 'float:left'},
                          table({class => 'strip'},
                                Tr(td(&thumbnail($l->{files}{$f}))),
                                Tr(td($f))));
          }
          else {
            push @additional,Tr(td([$f,$l->{files}{$f}]));
          }
        }
        $fcontent .= div({style => 'float:left; margin-left:15px'},$strip)
                     . div({style => 'clear: both;'},NBSP) if ($strip);
        $fcontent .= br . table(@additional) if (scalar @additional);
        delete $l->{files};
        $fcontent = &notAuthorized() unless ($AUTHORIZED);
      }
      $tcontent .= div({style => 'margin-left: 15px;'},
                       h4(&toggle($lid,$arrow),$n),
                       div({&identify($lid),
                            style => $display},$fcontent,
                           pre(to_json($l,{utf8 => 1, pretty => 1}))));
    }
    $tcontent = div({style => 'margin-left:15px'},"LSMs: ". scalar(@{$t->{lsmReferences}})) . $tcontent;
    delete $t->{lsmReferences};
  }
  delete $t->{files} if (exists($t->{files}) && !scalar(keys %{$t->{files}}));
  $tcontent .= &process_files($t) if (exists($t->{files}));
  $tcontent .= pre(to_json($t,{utf8 => 1, pretty => 1}))
    if (scalar(keys %$t));
  return($tcontent);
}


sub process_files
{
  my $tf = shift;
  my ($fcontent,$strip) = ('')x2;
  my @additional;
  foreach my $f (sort keys %{$tf->{files}}) {
    if ($tf->{files}{$f} =~ /\.png$/) {
      $strip .= div({style => 'float:left'},
                    table({class => 'strip'},
                          Tr(td(&thumbnail($tf->{files}{$f}))),
                          Tr(td($f))));
    }
    else {
      push @additional,Tr(td([$f,$tf->{files}{$f}]));
    }
  }
  delete $tf->{files};
  $fcontent .= div({style => 'float:left; margin-left:15px;'},$strip)
               . div({style => 'clear: both;'},NBSP) if ($strip);
  $fcontent = &notAuthorized() unless ($AUTHORIZED);
  return($fcontent);
}


sub thumbnail
{
  my($path) = @_;
  my $src = "$SERVER{'jacs-storage'}{address}$path";
  a({href => $src,
     target => '_blank'},
    img({src => $src,
         width => '100px'}));
}


sub toggle
{
  my($id,$dir) = @_;
  a({href => '#',
     onClick => 'toggleVis("'.$id.'"); return false;'},
    img({&identify('i'.$id),
         src => '/images/' . $dir . '_triangle_small.png'}));
}


sub notAuthorized
{
  div({class => "alert alert-danger", role => "alert"},
      span({ class => "glyphicon glyphicon-exclamation-sign", 'aria-hidden' => "true"}),
      span({class => "sr-only"},'Error:'),
      'You are not authorized to view imagery for this sample');
}


sub getSample
{
  my($type,$id) = @_;
  my $html = h2(ucfirst($type) . " $id") . br;
  $sth{ED}->execute($id);
  my $ar = $sth{ED}->fetchall_arrayref();
  my %sample;
  my $completion_time;
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
        $completion_time = $sample{$a}{'Completion Date'} if (exists $sample{$a}{'Completion Date'});
      }
      if ($line && $slide) {
        $sth{LSMS}->execute($slide,$line);
        $ar = $sth{LSMS}->fetchall_arrayref();
        $tasks .= h3('Associated LSM files')
                  . table({class => 'tablesorter standard'},
                          thead(Tr(th(['Name','Objective','Area','Tile']))),
                          tbody(map {$a = a({href => "view_sage_imagery.cgi?_op=stack;_family=$_->[0];_image=$_->[1]",
                                             target => '_blank'},$_->[1]);
                                     $a .= ' '
                                        . a({class => 'btn btn-info btn-xs',
                                             href => $_->[5]},'Download')
                                       if ($_->[5]);
                                     Tr(td([$a,@$_[2,3,4]]) )
                                    } @$ar)) if (scalar @$ar);
        if ($last_event eq 'completed' && $line && $slide) {
          my $ctm;
          $sth{CT}->execute($completion_time,$slide,$line);
          my($ct) = $sth{CT}->fetchrow_array();
          if ($ct) {
            my $c = sprintf 'tmog &rarr; Image processing completion cycle time: %d day%s',$ct,(1 == $ct) ? '' : 's';
            $ctm .= p({class => ($ct < 0) ? 'bg-danger' : 'bg-primary'},$c);
          }
          $sth{CT2}->execute($completion_time,$cross);
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
