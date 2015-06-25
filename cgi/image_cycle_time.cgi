#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use Getopt::Long;
use IO::File;
use POSIX qw(ceil strftime);
use Statistics::Basic qw(:all);
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Slime qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Image processing cycle time';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";
# Total days of history to fetch from the Workstation
my $WS_LIMIT_DAYS = 7;
my $WS_LIMIT_HOURS = 24 * $WS_LIMIT_DAYS;

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our ($dbh,$dbhw);

# ****************************************************************************
my $RUNMODE = ('apache' eq getpwuid($<)
              || 'daemon' eq getpwuid($<)) ? 'web' : 'command';

my($change_basedate,$change_days) = ('')x2;
if ($RUNMODE eq 'web') {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
  $change_days = param('days') if (param('days'));
  $change_basedate = param('basedate') if (param('basedate'));
}
else {
GetOptions('days=s'     => \$change_days,
           'basedate=s' => \$change_basedate,
           help         => \my $HELP)
  or pod2usage(-1);
}
# Adjust parms if necessary
if ($change_days) {
  $WS_LIMIT_DAYS = $change_days;
  $WS_LIMIT_HOURS = 24 * $WS_LIMIT_DAYS;
}
my $SUBTITLE = "last $WS_LIMIT_DAYS days";
my $BASEDATE = 'NOW()';
if ($change_basedate) {
  $BASEDATE = "CONCAT(DATE('" . $change_basedate . "'),' 00:00:00')";
  $SUBTITLE = "$WS_LIMIT_DAYS days prior to " . $change_basedate;
}
my %sth = (
tmog => 'SELECT i.family,ip2.value,i.create_date,'
        . 'TIMESTAMPDIFF(HOUR,?,i.create_date)/24 FROM '
        . 'image_vw i JOIN image_property_vw ip1 ON (i.id=ip1.image_id '
        . "AND ip1.type='slide_code') "
        . 'JOIN image_property_vw ip2 ON (i.id=ip2.image_id AND '
        . "ip2.type='data_set') WHERE i.line=? AND ip1.value=?",
# -----------------------------------------------------------------------------
WS_Pipeline => "SELECT e.id,e.name,ed.value,ed1.value,ed2.value,"
               . "t.event_timestamp FROM task_event t JOIN task_parameter tp "
               . "ON (tp.task_id=t.task_id AND "
               . "parameter_name='sample entity id') JOIN entity e ON "
               . "(e.id=tp.parameter_value) JOIN entityData ed ON "
               . "(e.id=ed.parent_entity_id AND ed.entity_att='Line') JOIN "
               . "entityData ed1 ON (e.id=ed1.parent_entity_id AND "
               . "ed1.entity_att='Slide Code') LEFT OUTER JOIN entityData ed2 "
               . "ON (e.id=ed2.parent_entity_id AND "
               . "ed2.entity_att='Cross Barcode'),(SELECT task_id,"
               . "MAX(event_no) event_no FROM task_event GROUP BY 1) x WHERE "
               . "x.task_id = t.task_id AND x.event_no = t.event_no AND "
               . "t.event_type='completed' AND "
               . "TIMESTAMPDIFF(HOUR,t.event_timestamp,$BASEDATE) "
               . "BETWEEN 0 AND $WS_LIMIT_HOURS ORDER BY 3",
WS_Event => "SELECT tp.parameter_value,COUNT(1) FROM task_event t JOIN task_parameter tp ON (tp.task_id=t.task_id AND parameter_name='sample entity id') WHERE t.event_type=? GROUP BY 1",
);



# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayCycleTime();
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'sage');
  &dbConnect(\$dbhw,'workstation');
  foreach (keys %sth) {
    if (/^WS/) {
      (my $n = $_) =~ s/WS_//;
      $sth{$n} = $dbhw->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
    else {
      $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    }
  }
}


sub displayCycleTime
{
  # Build HTML
  &printHeader() if ($RUNMODE eq 'web');
  my (%error,%first);
  $sth{Event}->execute('completed');
  my $ar = $sth{Event}->fetchall_arrayref();
  foreach (@$ar) {
    $first{$_->[0]}++ if ($_->[1] == 1);
  }
  $sth{Event}->execute('error');
  $ar = $sth{Event}->fetchall_arrayref();
  foreach (@$ar) {
    $error{$_->[0]}++ if ($_->[1]);
  }
  $sth{Pipeline}->execute();
  $ar = $sth{Pipeline}->fetchall_arrayref();
  my (%acc,%no_errors);
  my @ok;
  foreach (@$ar) {
    # ID, name, line, data set slide code, timestamp
    next unless (exists $first{$_->[0]});
    $sth{tmog}->execute($_->[5],$_->[2],$_->[3]);
    my @t = $sth{tmog}->fetchrow_array();
    # family, data set, tmog date
    next unless ($t[2]);
    push @$_,@t[1,2,3];
    $_->[-1] = ceil(abs($_->[-1]));
    push @ok,$_;
    $_->[1] = a({href => "sample_search.cgi?sample_id=$_->[1]",
                  target => '_blank'},$_->[1]);
    push @{$acc{$t[1]}},$_->[-1];
    unless (exists $error{$_->[0]}) {
      $no_errors{$t[1]}++;
      $no_errors{TOTAL}++;
    }
    push @{$acc{TOTAL}},$_->[-1];
  }
  if ($RUNMODE eq 'web') {
    print h2("Cycle time for completed samples from the $SUBTITLE");
    print scalar(@ok),' samples',br;
  }
  my @row;
  foreach my $ds (sort keys %acc) {
    next if ($ds eq 'TOTAL');
    my $t = 0;
    $t += $_ foreach (@{$acc{$ds}});
    $no_errors{$ds} ||= 0;
    my $count = scalar(@{$acc{$ds}});
    my $rar = [$ds,$count,(sprintf '%.2f%%',$no_errors{$ds}/$count*100),
               mean($acc{$ds}),stddev($acc{$ds})];
    ($RUNMODE eq 'web') ? push @row,td($rar) : push @row,$rar;
  }
  my $ds = 'TOTAL';
  my $count = scalar(@{$acc{$ds}});
  my @CT_HEAD = ('Data set','Samples','% first time success',
                 'Avg cycle time (days)','Std Dev');
  if ($RUNMODE eq 'web') {
    print h3("tmog &rarr; Image processing cycle time"),
          table({id => 'stats',class => 'tablesorter standard'},
                thead(Tr(th(\@CT_HEAD))),
                tbody(map {Tr($_)} @row),
                tfoot(Tr(td([$ds,$count,(sprintf '%.2f%%',
                             $no_errors{$ds}/$count*100),mean($acc{$ds}),
                             stddev($acc{$ds})]))));
#    print table({class => 'standard'},map {Tr(td($_))} @ok);
    print &createExportFile(\@ok,"_cycle_time",['ID','Sample','Line',
                                                'Slide code','Cross barcode',
                                                'Completion date','Data set',
                                                'tmog date','Cycle time (days)']);
    print end_form,&sessionFooter($Session),end_html;
  }
  else {
    print join("\t",@CT_HEAD) . "\n";
    push @row,[$ds,$count,(sprintf '%.2f%%',
               $no_errors{$ds}/$count*100),mean($acc{$ds}),
               stddev($acc{$ds})];
    foreach (@row) {
      print join("\t",@$_) . "\n";
    }
  }
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d%H:%M:%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach (@$ar) {
    my @l = @$_;
    foreach my $i (1) {
      if ($l[$i] =~ /href/) {
        $l[$i] =~ s/.+=//;
        $l[$i] =~ s/".+//;
      }
    }
    $l[4] ||= ''; # Cross barcode
    print $handle join("\t",@l) . "\n";
  }
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export data");
  return($link);
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('highcharts-4.0.1/highcharts','jquery/jquery.tablesorter',
                     'tablesorter');
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
