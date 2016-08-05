#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use Data::Dumper;
use DBI;
use IO::File;
use JSON;
use LWP::Simple;
use POSIX qw(ceil strftime);
use Time::HiRes qw(gettimeofday tv_interval);
use Time::Local;
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);
$|++;

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Image processing pipeline status';
my %CONFIG;
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my %STEP;
my @STEPS;
my $BASE = "/var/www/html/output/";
my $ROW_LIMIT = 5000;
# Total days of history to fetch from the Workstation
my $WS_LIMIT_DAYS = param('days') || 14;
my $WS_LIMIT_HOURS = -24 * $WS_LIMIT_DAYS;
# Stale warnings
my %STALE = (Pipeline => {process => {limit => 48, class => 'danger'}});

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Database
our ($dbh,$dbhf,$dbhw);
my $MONGO = 0;
my %sth = (
FB_tmog => "SELECT event_date,stock_name,cross_stock_name2,cross_effector,"
           . "seh.cross_type,seh.lab_project,cross_barcode,seh.wish_list FROM "
           . "stock_event_history_vw seh JOIN Project_Crosses pc ON "
           . "(pc.__kp_ProjectCrosses_Serial_Number = cross_barcode) WHERE "
           . "event='cross' AND seh.project='Fly Light' AND "
           . "seh.lab_project != 'Truman' AND RedoCheckbox IS NULL AND "
           . "TIMESTAMPDIFF(DAY,NOW(),event_date) BETWEEN -90 AND -14 "
           . "ORDER BY event_date",
# -----------------------
WS_Status => "SELECT value,COUNT(1) FROM entityData WHERE entity_att='Status' GROUP BY 1",
Indexing => 'SELECT i.family,i.line,ip1.value,i.name,ip2.value,i.create_date '
            . 'FROM image_vw i JOIN image_property_vw ip1 ON '
            . "(i.id=ip1.image_id AND ip1.type='slide_code') "
            . 'JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND '
            . "ip2.type='data_set') WHERE i.family NOT LIKE 'simpson%' AND "
            . 'i.id NOT IN (SELECT image_id FROM image_property_vw WHERE '
            . "type='bits_per_sample') ORDER BY 6",
MV => 'SELECT i.family,i.line,ip1.value,i.name,ip2.value,i.create_date FROM '
      . 'image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id '
      . "AND ip1.type='slide_code') JOIN image_property_vw ip2 ON "
      . "(i.id=ip2.image_id AND ip2.type='data_set') WHERE "
      . 'TIMESTAMPDIFF(DAY,NOW(),i.create_date)=0 AND '
      . 'i.id NOT IN (SELECT id FROM image_data_mv) ORDER BY 6',
Discovery => 'SELECT i.family,i.line,ip1.value,i.name,ip2.value,'
             . 'i.create_date,TIMESTAMPDIFF(HOUR,NOW(),i.create_date) FROM '
             . 'image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id '
             . "AND ip1.type='slide_code') "
             . 'JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND '
             . "ip2.type='data_set') WHERE TIMESTAMPDIFF(HOUR,NOW(),"
             . "i.create_date) >= $WS_LIMIT_HOURS ORDER BY 6",
# -----------------------------------------------------------------------------
WS_Tasking => "SELECT e.name,ed1.value,e.creation_date,"
              . "TIMESTAMPDIFF(HOUR,NOW(),e.creation_date) FROM entity e "
              . "LEFT OUTER JOIN entityData ed ON (e.id=ed.parent_entity_id "
              . "AND ed.entity_att='Status') JOIN entityData ed1 ON "
              . "(e.id=ed1.parent_entity_id AND "
              . "ed1.entity_att='Data Set Identifier') WHERE "
              . "e.entity_type='Sample' AND TIMESTAMPDIFF(HOUR,NOW(),"
              . "e.creation_date) >= $WS_LIMIT_HOURS AND ed.value IS NULL "
              . "AND e.name NOT LIKE '%~%' ORDER BY 3",
WS_Pipeline => "SELECT * FROM (SELECT e.name,ed.value,t.description,"
               . "t.event_timestamp,TIMESTAMPDIFF(HOUR,NOW(),"
               . "t.event_timestamp),t.event_type FROM task_event t "
               . "JOIN task_parameter tp ON (tp.task_id=t.task_id AND "
               . "parameter_name='sample entity id') JOIN entity e ON "
               . "(e.id=tp.parameter_value) JOIN entityData ed ON "
               . "(e.id=ed.parent_entity_id AND "
               . "entity_att='Data Set Identifier') JOIN task ON "
               . "(task.task_id=tp.task_id AND "
               . "task.task_name!='SyncSampleToScality') WHERE "
               . "TIMESTAMPDIFF(HOUR,NOW(),t.event_timestamp) >= "
               . "$WS_LIMIT_HOURS ORDER BY 4 DESC,event_no DESC) x GROUP BY 1",
# -----------------------------------------------------------------------------
Barcode => "SELECT DISTINCT value FROM image_property_vw WHERE "
           . "type='cross_barcode'",
WS_Entity => "SELECT id FROM entity WHERE entity_type='LSM stack' AND name=?",
WS_Error => "SELECT s.name,IFNULL(ced.value,'UnclassifiedError') "
            . "classification, ded.value description FROM entity e "
            . "LEFT OUTER JOIN entityData ced ON ced.parent_entity_id=e.id AND "
            . "ced.entity_att='Classification' LEFT OUTER JOIN entityData ded "
            . "ON ded.parent_entity_id=e.id AND ded.entity_att='Description' "
            . "JOIN entityData pred ON pred.child_entity_id=e.id JOIN "
            . "entityData ssed ON pred.parent_entity_id=ssed.child_entity_id "
            . "JOIN entityData sed ON "
            . "ssed.parent_entity_id=sed.child_entity_id JOIN entity s ON "
            . "ssed.parent_entity_id=s.id AND s.entity_type='Sample' WHERE "
            . "e.entity_type='Error' AND s.name NOT LIKE '%~%' UNION "
            . "SELECT s.name, IFNULL(ced.value,'UnclassifiedError') "
            . "classification, ded.value description FROM entity e "
            . "LEFT OUTER JOIN entityData ced ON ced.parent_entity_id=e.id AND "
            . "ced.entity_att='Classification' LEFT OUTER JOIN entityData ded "
            . "ON ded.parent_entity_id=e.id AND ded.entity_att='Description' "
            . "JOIN entityData pred ON pred.child_entity_id=e.id JOIN "
            . "entityData ssed ON pred.parent_entity_id=ssed.child_entity_id "
            . "JOIN entityData sed ON "
            . "ssed.parent_entity_id=sed.child_entity_id JOIN entity s ON "
            . "sed.parent_entity_id=s.id AND s.entity_type='Sample' WHERE "
            . "e.entity_type='Error'",
);
my $performance = '';

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
our $USERID = $Session->param('user_id');
our $USERNAME = $Session->param('user_name');
my $SCICOMP = ($Session->param('scicomp'));

my $ALL = param('all') || 0;
&initializeProgram();
&displayQueues();

# We're done!
if ($dbh) {
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
  my $p;
  eval {
    $p = XMLin(DATA_PATH . $PROGRAM . '-config.xml',
             KeyAttr => {});
  };
  &terminateProgram('XML error: '.$@) if ($@);
  @STEPS = map { $_->{name} } @{$p->{step}};
  %STEP = map { $_->{name} => $_ } @{$p->{step}};
  # Get WS REST config
  my $file = DATA_PATH . 'workstation_ng.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %CONFIG = %$hr;
  $MONGO = (param('mongo')) || ('mongo' eq $CONFIG{data_source});
  $MONGO = 0 if (param('mysql'));
  # Connect to databases
  &dbConnect(\$dbh,'sage');
  &dbConnect(\$dbhf,'flyboy');
  &dbConnect(\$dbhw,'workstation') unless ($MONGO);
  foreach (keys %sth) {
    if (/^WS/) {
      unless ($MONGO) {
        (my $n = $_) =~ s/WS_//;
        $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
      }
    }
    elsif (/^FB/) {
      (my $n = $_) =~ s/FB_//;
      $sth{$n} = $dbhf->prepare($sth{$_}) || &terminateProgram($dbhf->errstr)
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
  }
}


sub displayQueues
{
  # Build HTML
  &printHeader();
  my (%process,%queue);
  # Get errors
  my $ar0;
  if ($MONGO) {
    my $rvar = &getMONGO('SampleErrorsAll');
    foreach (@$rvar) {
      push @$ar0,[@{$_}{qw(sampleName classification)},$_->{description}||''];
    }
  }
  else {
    $sth{Error}->execute();
    $ar0 = $sth{Error}->fetchall_arrayref();
  }
  my %error;
  foreach (@$ar0) {
    $error{$_->[0]} = [$_->[1],$_->[2]];
  }

  shift(@STEPS) unless ($ALL);
  foreach my $s (@STEPS[1..$#STEPS]) {
    next if (($s eq 'MV') && !$ALL);
    next if ($s eq 'Scheduling');
    my $ar;
    my $t0 = [gettimeofday];
    if ($MONGO && ($s =~ /(Tasking|Pipeline)/)) {
      my $rvar = &getMONGO($s);
      if ($s eq 'Tasking') {
        foreach (sort {$a->{creationDate} cmp $b->{creationDate}} @$rvar) {
          push @$ar,[@{$_}{qw(name dataSet creationDate pipelineTime)}];
        }
      }
      elsif ($s eq 'Pipeline') {
        foreach (sort {$b->{timestamp} cmp $a->{timestamp}} @$rvar) {
          push @$ar,[@{$_}{qw(name dataSet description timestamp age eventType)}];
        }
      }
    }
    else {
      $sth{$s}->execute();
      $ar = $sth{$s}->fetchall_arrayref;
    }
    if ($s eq 'Discovery') {
      my @arr = @$ar;
      @$ar = ();
      foreach (@arr) {
        my $stack = (split(/\//,$_->[3]))[-1];
        next unless ($stack =~ /lsm$/);
        my $p;
        if ($MONGO) {
          my $rvar = &getMONGO('Entity',$stack);
          $p = $rvar;
        }
        else {
          $sth{Entity}->execute($stack);
          ($p) = $sth{Entity}->fetchrow_array();
        }
        push @$ar,$_ unless ($p);
      }
    }
    elsif ($s eq 'tmog') {
      my @arr = @$ar;
      @$ar = ();
      $sth{Barcode}->execute();
      my $bc = $sth{Barcode}->fetchall_arrayref();
      my %bh = map {$_->[0] => 1} @$bc;
      foreach (@arr) {
        push @$ar,$_ unless (exists $bh{$_->[6]});
      }
    }
    foreach (@$ar) {
      if ($s eq 'Pipeline') {
        my $event = pop(@$_);
        my $sam = $_->[0];
        $_->[0] = a({href => "sample_search.cgi?sample_id=" . $_->[0],
                     target => '_blank'},$_->[0]) if ($_->[0]);
        if ($event eq 'created') {
          splice(@$_,2,1);
          push @{$queue{Scheduling}},$_;
        }
        elsif ($event eq 'pending') {
          push @{$queue{$s}},$_;
        }
        elsif ($event eq 'running') {
          push @{$process{$s}},$_;
        }
        elsif ($event eq 'completed') {
          pop(@$_);
          push @{$queue{Complete}},$_;
        }
        elsif ($event eq 'error') {
          pop(@$_);
          splice(@$_,2,0,'Unknown');
          if (exists $error{$sam}) {
            $_->[2] = $error{$sam}[0];
            $_->[3] = $error{$sam}[1];
          }
          push @{$process{Pipeline_Error}},$_;
        }
      }
      elsif ($s eq 'Tasking') {
        $_->[0] = a({href => "sample_search.cgi?sample_id=" . $_->[0],
                     target => '_blank'},$_->[0]);
        push @{$queue{$s}},$_;
      }
      elsif ($s eq 'tmog') {
        $_->[0] =~ s/ .*//;
        foreach my $i (1..2) {
          $_->[$i] ||= '';
          $_->[$i] = a({href => "lineman.cgi?line=" . $_->[$i],
                        target => '_blank'},$_->[$i]) if ($_->[$i]);
        }
        $_->[6] = a({href => "/flyboy_search.php?kcross=" . $_->[6],
                     target => '_blank'},$_->[6]);
        push @{$queue{$s}},$_;
      }
      else {
        my $f = shift @$_;
        my $i = 0;
        $_->[$i] = a({href => "lineman.cgi?line=" . $_->[$i],
                      target => '_blank'},$_->[$i]);
        $i++;
        $_->[$i] = a({href => "/slide_search.php?term=slide_code&id=" . $_->[$i],
                      target => '_blank'},$_->[$i]);
        $i++;
        $_->[$i] = a({href => "view_sage_imagery.cgi?_op=stack;_family=$f;_image="
                              . $_->[$i],
                      target => '_blank'},$_->[$i]);
        push @{$queue{$s}},$_;
      }
    }
    my $msg = sprintf "$s processing for %d rows: %.2fsec",
                      scalar(@$ar),tv_interval($t0,[gettimeofday]);
    print STDERR "$msg\n";
    $performance .= $msg . '<br>';
  }
  # Display
  my $t0 = [gettimeofday];
  my (@details,@special,@status);
  foreach my $step (@STEPS,'Complete') {
    next if (($step eq 'MV') && !$ALL);
    my ($block,$data,$data2);
    unless ($step eq $STEPS[0]) {
      ($block,$data) = &stepContents(\%queue,$step,'queue');
      push @status,$block;
      push @details,$data;
    }
    unless ($step eq 'Complete') {
      ($block,$data,$data2) = &stepContents(\%process,$step,'process');
      push @status,$block;
      push @details,$data;
      push @details,$data2 if ($data2);
    }
  }
  my $msg = sprintf "Queue population: %.2fsec",
                    tv_interval($t0,[gettimeofday]);
  print STDERR "$msg\n";
  $t0 = [gettimeofday];
  $performance .= $msg . '<br>';
  $performance = '' unless (param('performance'));
  my $instructions = <<__EOT__;
<h3>Instructions</h3><br>
The diagram to the left shows the image processing pipeline, with rounded boxes
representing process steps. The areas between the process steps are queues;
holding areas for items that have completed a process step
but not yet started another. An "empty" in a queue area indicates that
there are no items waiting to enter the next process step, a
number indicates that there are that many waiting.
<br><br>
An item will be one of the following:
<ul>
__EOT__
  $instructions .= '<li>Up to tmog: cross barcode</li>' if ($ALL);
  $instructions .= <<__EOT__;
<li>Indexing: image (LSM)</li>
<li>Discovery and subsequent steps: sample</li>
</ul>
<br>
Clicking on any of the numbers found in the queue or process areas to the
left will display details on the items at that particular
point in the process flow.
Completion and error data includes only samples from the last $WS_LIMIT_DAYS days.
<br><br>
Legend:<br>
<span class="badge badge-full">&nbsp;&nbsp;&nbsp;</span> Items < 2 days old<br>
<span class="badge badge-warning">&nbsp;&nbsp;&nbsp;</span> Items 2-7 days old<br>
<span class="badge badge-late">&nbsp;&nbsp;&nbsp;</span> Items >= 7 days old<br>
<span class="badge badge-complete">&nbsp;&nbsp;&nbsp;</span> Samples that completed processing<br>
<span class="badge badge-error">&nbsp;&nbsp;&nbsp;</span> Samples that did not complete processing<br>
__EOT__
  unshift @details,div({&identify('instructions')},$instructions);
  print div({style => 'clear: both;'},NBSP),&statusTotals();
  print div({class => 'boxed',style => 'float:left;width:100%'},
            div({align => 'center'},
                h2(a({href => '#',
                      onclick => "showDetails('instructions')"},$APPLICATION." (last $WS_LIMIT_DAYS days)"))),
                br,
            div({style => 'float: left;'},
                $performance,
                div({style => 'float: left;'},
                    div({class => 'flow'},
                        join(div({class => 'downarrow'},'&darr;'),@status)),
                    div({style => 'clear: both;'},NBSP),
                    div(join(NBSP,@special))),
                div({class => 'details'},join('',@details))
               ),
           ),
           div({style => 'clear: both;'},NBSP);
  print end_form,&sessionFooter($Session),end_html;
  $msg = sprintf "Final print: %.2fsec",
                 tv_interval($t0,[gettimeofday]);
  print STDERR "$msg\n";
}


sub getMONGO
{
  my($selector,$value) = @_;
  my $suffix = '';
  if ($selector eq 'Tasking') {
    $selector = 'PipelineStatus';
    $suffix = '?hours=' . abs($WS_LIMIT_HOURS);
  }
  elsif ($selector eq 'Entity') {
    $suffix = "?name=$value";
  }
  elsif ($selector eq 'Pipeline') {
    $selector = 'TasksLatest';
    $suffix = '?hours=' . abs($WS_LIMIT_HOURS);
  }
  my $t0 = [gettimeofday];
  my $rest = $CONFIG{url}.$CONFIG{query}{$selector} . $suffix;
  my $response = get $rest;
  if ($selector eq 'Entity') {
    return((length($response)) ? 1 : 0);
  }
  else {
    $performance .= sprintf "REST GET $selector: %.2fsec<br>",
                            tv_interval($t0,[gettimeofday])
  }
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  return($rvar);
}


sub statusTotals
{
my %STATUS = (Complete => '090',
              Error => 'f33');
  my %GOOD = map {$_ => 1} qw(Blocked Complete Retired);
  $STATUS{$_} = '39c' foreach(qw(Blocked Retired));
  $STATUS{$_} = 'f93' foreach('Desync','Marked for Rerun','Processing');
  my $ar;
  if ($MONGO) {
    my $rest = $CONFIG{url}.$CONFIG{query}{SampleStatus};
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    foreach (@$rvar) {
      push @$ar,[@{$_}{qw(_id count)}] if ($_->{_id});
    }
  }
  else {
    $sth{Status}->execute();
    $ar = $sth{Status}->fetchall_arrayref();
  }
  my $total = 0;
  my(@bad,@good);
  $total += $_->[1] foreach (@$ar);
  foreach (@$ar) {
    my $text = sprintf '%s: %s (%.2f%%)',
               span({style => 'color: #'.($STATUS{$_->[0]}||'ccc')},$_->[0]),
               &commify($_->[1]),$_->[1]/$total*100;
    if (exists $GOOD{$_->[0]}) {
      push @good,$text;
    }
    else {
      push @bad,$text;
    }
  }
  my $warn = '';
  foreach my $s (keys %STALE) {
    foreach my $t (keys %{$STALE{$s}}) {
      if (exists $STALE{$s}{$t}{count}) {
        $warn .= br . span({class => 'label label-'.($STALE{$s}{$t}{class}||'danger')},
                           'Warning') . NBSP
                 . sprintf 'The %s %s has %d stale entr%s (>= %d hours)',
                   ($s,$t,$STALE{$s}{$t}{count},($STALE{$s}{$t}{count} == 1) ? 'y' : 'ies',$STALE{$s}{$t}{limit});
      }
    }
  }
  $warn = br . $warn if ($warn);
  div({class => 'boxed'},
      div({align => 'center'},h2('Overall sample status')),
      join((NBSP)x3,@good),br,join((NBSP)x3,@bad),$warn);
}


sub stepContents
{
  my($href,$step,$type) = @_;
  my $items = (exists $href->{$step}) ? scalar @{$href->{$step}} : 0;
  my ($table,$table2) = ('')x2;
  my $type_title = ($type eq 'special') ? '' : $type;
  my $now = strftime "%Y-%m-%d %H:%M:%S",localtime;
  my $head = ['Line','Slide code','Image','Data set','tmog date'];
  my ($js,$state) = ('','');
  if ($items) {
    ($state,$js) = &generateHistograms($step,$href)
      if ($step =~ /(?:Discovery|Tasking|Scheduling|Pipeline)/);
    if ($step eq 'Tasking') {
      $head = ['Sample ID','Data set','Discovery date'];
    }
    if ($step eq 'Scheduling') {
      $head = ['Sample ID','Data set','Tasking date'];
    }
    elsif ($step eq 'tmog') {
      $head = ['Cross date','Line','Line 2','Effector','Type','Project','Barcode','Wish list'];
      $state = 'late';
    }
    elsif ($step eq 'Pipeline') {
      $head = ($type eq 'queue')
        ? ['Sample ID','Data set','Description','Scheduling date']
        : ['Sample ID','Data set','Description','Pipeline start'];
    }
    elsif ($step eq 'Complete') {
      $head = ['Sample ID','Data set','Description','Completion date'];
    }
    # Find stale process/queue entries
    my $now = time;
    if ((exists $STALE{$step}) && (exists $STALE{$step}{$type})) {
      foreach (@{$href->{$step}}) {
        my @f = split(/[-: ]/,$_->[-1]);
        $f[1]--;
        my $then = timelocal(reverse @f);
        my $delta_hours = ($now - $then) / 3600;
        $STALE{$step}{$type}{count}++ if ($delta_hours >= $STALE{$step}{$type}{limit});
      }
    }
    # Create export file
    my $link = &createExportFile($href->{$step},"_$step",$head);
    # Create process/queue table
    $type_title = "$type_title ($STEP{$step}{description})"
      unless ($step =~ /(?:Complete)/);
    if (scalar(@{$href->{$step}}) > $ROW_LIMIT) {
      $table = h3("$step $type_title") . $link . br . $js . br
               . "There are too many entries to display - please "
               . "download the Export file.";
    }
    else {
      $table = h3("$step $type_title") . $link . br . $js .
               table({id => "t$step",class => 'tablesorter standard'},
                     thead(Tr(th($head))),
                     tbody (map {Tr(td($_))}
                            @{$href->{$step}}));
    }
  }
  my $badge_type = 'empty';
  if ($items) {
    $badge_type = 'full';
    if ($state) {
      $badge_type = $state;
    }
    elsif ($step eq $STEPS[0] && $type eq 'queue') {
      $badge_type = 'start';
    }
    elsif ($step eq 'Complete') {
      $badge_type = 'complete';
    }
  }
  my $badge = ($type eq 'queue')
    ? span({class => "badge badge-$badge_type"},$items||'empty')
    : (($items) ? ' '.span({class => "badge badge-$badge_type"},$items) : '');
  if ($badge =~ /badge/ && $badge !~ /empty/) {
    $badge = a({href => '#',
                onclick => "showDetails('$type" . '_' . "$step')"},
               $badge);
  }
  $badge = $step . $badge if ($type ne 'queue');
  my $style = $STEP{$step}{style} || '';
  $style = '' unless ($type eq 'process');
  my $estep = $step . '_Error';
  if ($type eq 'process' && exists($href->{$estep})) {
    # Error queue
    my %count;
    $count{$_->[2]}++ foreach @{$href->{$estep}};
    my $total = scalar @{$href->{$estep}};
    $count{$_} = (sprintf '%.2f%%',$count{$_}/$total*100) foreach (keys %count);
    $head = ['Sample ID','Data set','Class','Description','Error date'];
    my $link = &createExportFile($href->{$estep},"_$estep",$head);
    $table2 = h3("$step process (Errors)") . $link 
              . &generateFilter($href->{$estep},\%count) . $js .
              table({id => "t$estep",class => 'tablesorter standard'},
                    thead(Tr(th($head))),
                    tbody (map {Tr({class => $_->[2]},td($_))}
                           @{$href->{$estep}}));
    $badge = div({class => $type,style => "float: left; $style"},$badge);
    my $badge2 = a({href => '#',
                    onclick => "showDetails('$type" . '_' . "$estep')"},
                   div({class => "badge badge-error"},$total));
    $badge = div({style => "float: left"},
                 $badge,
                 div({class => 'rightarrow'},'&rarr;'),
                 div({class => 'error_queue',style => "float: left"},$badge2)
                );
  }
  else {
    $badge = div({class => $type,style => $style},$badge);
  }
  return($badge,
         div({&identify($type.'_'.$step),class => 'detailarea'},$table),
         ($table2) ? div({&identify($type.'_'.$step.'_Error'),class => 'detailarea'},$table2) : '');
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
    foreach my $i (0..2,3..6) {
      $l[$i] ||= '';
      if ($l[$i] && ($l[$i] =~ /href/)) {
        $l[$i] =~ s/.+=//;
        $l[$i] =~ s/".+//;
      }
    }
    print $handle join("\t",@l) . "\n";
  }
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export data");
  return($link);
}


sub generateHistograms
{
  my($step,$href) = @_;
  my %hist = ();
  my %hist2 = ();
  my ($delta,$state) = ('')x2;
  my $first = 'na';
  my $ind = ('Tasking' eq $step) ? 1 : 3;
  foreach (@{$href->{$step}}) {
    my $l = pop @$_;
    $_->[$ind] = 0 unless ($_->[$ind] && length($_->[$ind]));
    $first = abs($l) if ($first eq 'na');
    $delta = abs($l) / 24;
    $hist{ceil($delta)}++;
    $hist2{$_->[$ind]}++;
  }
  $delta = $first / 24;
  if ($delta >= 7) {
    $state = 'late';
  }
  elsif ($delta >= 2) {
    $state = 'warning';
  }
  return($state,'') unless ($step =~ /(?:Discovery|Tasking)/);
  my $container = $step . '_container';
  my $container2 = $step . '_container2';
  my $js = <<__EOT__;
<div id="$container" style="height: 200px"></div>
<div id="$container2" style="height: 200px"></div>
<script type="text/javascript">
\$(function () {
var chart = new Highcharts.Chart({
chart: {renderTo: '$container',
        type: 'bar'},
title: {text: 'LSMs by age'},
credits: {enabled: false},
legend: {enabled: false},
yAxis: {title: {text: '# LSMs'}},
xAxis: {title: {text: 'Days'},
        categories: [
__EOT__
      $hist{1} += delete($hist{0}) if (exists $hist{0});
      my @yaxis = map {'(' . ($_-1) .  " - $_]"} sort { $a <=> $b} keys %hist;
      $yaxis[0] = '<= 1' if ($yaxis[0] eq '(0 - 1]');
      $js .= "'" . join("','",@yaxis) . "'"
             . ']},series: [{data: ['
             . join(',',@hist{sort { $a <=> $b} keys %hist})
             . '],color: "#3cc"}]});';
      $js .= <<__EOT__;
var chart2 = new Highcharts.Chart({
chart: {renderTo: '$container2',
        type: 'bar'},
title: {text: 'LSMs by data set'},
credits: {enabled: false},
legend: {enabled: false},
yAxis: {title: {text: '# LSMs'}},
xAxis: {title: {text: 'Data set'},
        categories: [
__EOT__
      $js .= "'" . join("','",sort keys %hist2) . "'"
             . ']},series: [{data: ['
             . join(',',@hist2{sort keys %hist2})
             . '],color: "#3c3"}]});';
      $js .= '});</script>';
  return($state,$js);
}


sub generateFilter
{
  my($arr,$href) = @_;
  my %filt;
  $filt{$_->[2]}++ foreach (@$arr);
  my $html = join((NBSP)x4,
                  map { checkbox(&identify('show_'.$_),
                                 -label => " $_ (".$href->{$_}.')',
                                 -checked => 1,
                                 -onClick => "toggleClass('$_');")
                      } sort keys %filt);
  div({class => 'bg-info'},'Filter: ',(NBSP)x5,$html);
}


sub commify
{
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('chosen.jquery.min','highcharts-4.0.1/highcharts','jquery/jquery.tablesorter','tablesorter',$PROGRAM);
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(chosen.min tablesorter-jrc1);
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
