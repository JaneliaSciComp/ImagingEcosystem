#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use Getopt::Long;
use IO::File;
use POSIX qw(strftime);
use Switch;
use XML::Simple;
use JFRC::Utils::DB qw(:all);
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
our $APPLICATION = 'Image processing errors';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our $dbh;

# ****************************************************************************
my $RUNMODE = ('apache' eq getpwuid($<)
              || 'daemon' eq getpwuid($<)) ? 'web' : 'command';

if ($RUNMODE eq 'web') {
  # Session authentication
  $Session = &establishSession(css_prefix => $PROGRAM);
  &sessionLogout($Session) if (param('logout'));
  $USERID = $Session->param('user_id');
  $USERNAME = $Session->param('user_name');
}
else {
GetOptions(help => \my $HELP)
  or pod2usage(-1);
}
my %sth = (
ERRORS => "SELECT s.name,ds.value,IFNULL(ced.value,'UnclassifiedError') classification, ded.value description FROM entity e LEFT OUTER JOIN entityData ced ON (ced.parent_entity_id=e.id AND ced.entity_att='Classification') LEFT OUTER JOIN entityData ded ON (ded.parent_entity_id=e.id AND ded.entity_att='Description') JOIN entityData pred ON (pred.child_entity_id=e.id) JOIN entityData ssed ON (pred.parent_entity_id=ssed.child_entity_id) JOIN entityData sed ON (ssed.parent_entity_id=sed.child_entity_id) JOIN entity s ON (ssed.parent_entity_id=s.id AND s.entity_type='Sample') LEFT OUTER JOIN entityData ds ON (ds.parent_entity_id=s.id AND ds.entity_att='Data Set Identifier') WHERE e.entity_type='Error' AND s.name NOT LIKE '%~%' UNION SELECT s.name,ds.value, IFNULL(ced.value,'UnclassifiedError') classification, ded.value description FROM entity e LEFT OUTER JOIN entityData ced ON ced.parent_entity_id=e.id AND ced.entity_att='Classification' LEFT OUTER JOIN entityData ded ON ded.parent_entity_id=e.id AND ded.entity_att='Description' JOIN entityData pred ON pred.child_entity_id=e.id JOIN entityData ssed ON pred.parent_entity_id=ssed.child_entity_id JOIN entityData sed ON ssed.parent_entity_id=sed.child_entity_id JOIN entity s ON (sed.parent_entity_id=s.id AND s.entity_type='Sample') LEFT OUTER JOIN entityData ds ON (ds.parent_entity_id=s.id AND ds.entity_att='Data Set Identifier') WHERE e.entity_type='Error' ORDER BY 1",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayErrors();
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'workstation');
  foreach (keys %sth) {
    $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  }
}


sub displayErrors
{
  # Build HTML
  &printHeader() if ($RUNMODE eq 'web');
  $sth{ERRORS}->execute();
  my $ar = $sth{ERRORS}->fetchall_arrayref();
  # Name, data set, class, description
  my (%count,%stat);
  my @row;
  foreach (@$ar) {
    push @row,[@$_];
    if ($RUNMODE eq 'web') {
      $row[-1][0] = a({href => "sample_search.cgi?sample_id=$row[-1][0]",
                       target => '_blank'},$row[-1][0]);
      $count{Class}{$row[-1][2]}++;
      my $desc = $row[-1][3];
      if ($row[-1][2] eq 'LabError') {
        switch ($desc) {
          case /Could not find existing JSON metadata/ {
            $desc = 'Could not find existing JSON metadata' }
          case /has differing numbers of images per tile/ {
            $desc = 'Sample has differing numbers of images per tile' }
          case /No channel mapping consensus among tiles/ {
            $desc = 'No channel mapping consensus among tiles' }
        }
      }
      $stat{$row[-1][2]}{$desc}++;
    }
  }
  my @HEAD = ('Sample ID','Data set','Class','Description');
  if ($RUNMODE eq 'web') {
    my @stat;
    foreach my $c (sort keys %stat) {
      foreach (sort keys %{$stat{$c}}) {
        push @stat,[$c,$_,$stat{$c}{$_}];
      }
    }
    print "Errors: ",scalar @$ar,(NBSP)x5,
          &createExportFile($ar,"_ws_errors",\@HEAD),
          &generateFilter($ar,2,$count{Class}),br,
         table({id => 'stats',class => 'tablesorter standard'},
               thead(Tr(th(['Class','Description','Count']))),
               tbody(map {Tr({class => $_->[0]},td($_))} @stat)),
          table({id => 'details',class => 'tablesorter standard'},
                thead(Tr(th(\@HEAD))),
                tbody(map {Tr({class => $_->[2]},td($_))} @row),
               );
    print end_form,&sessionFooter($Session),end_html;
  }
  else {
    print join("\t",@HEAD) . "\n";
    foreach (@row) {
      print join("\t",@$_) . "\n";
    }
  }
}


sub generateFilter
{
  my($arr,$index,$href) = @_;
  my %filt;
  $filt{$_->[$index]}++ foreach (@$arr);
  my $html = join((NBSP)x4,
                  map { checkbox(&identify('show_'.$_),
                                 -label => " $_ (".$href->{$_}.')',
                                 -checked => 1,
                                 -onClick => "toggleClass('$_');")
                      } sort keys %filt);
  div({class => 'bg-info'},'Filter: ',(NBSP)x5,$html);
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
                     'tablesorter',$PROGRAM);
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
