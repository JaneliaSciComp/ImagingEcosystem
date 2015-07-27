#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use IO::File;
use POSIX qw(strftime);
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
our $APPLICATION = 'Image sample errors';
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
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
&terminateProgram('You are not authorized to view Workstation imagery')
  unless ($Session->param('scicomp'));
our $DATASET = param('dataset') || '';
my $OPTIONS = ($DATASET ne 'All datasets') ? "WHERE edd.value='$DATASET'"
                                             : '';
my %sth = (
DATASETS => "SELECT edd.value,COUNT(1) FROM entity e JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Status' AND eds.value='Error') JOIN entityData edd ON (e.id=edd.parent_entity_id AND edd.entity_att='Data Set Identifier') GROUP BY 1",
ERRORS => "SELECT DISTINCT e.name,edd.value,edi.value FROM entity e JOIN entityData eds ON (e.id=eds.parent_entity_id AND eds.entity_att='Status' AND eds.value='Error') JOIN entityData edd ON (e.id=edd.parent_entity_id AND edd.entity_att='Data Set Identifier') LEFT OUTER JOIN entityData edi ON (e.id=edi.parent_entity_id AND edi.entity_att='Default 2D Image') $OPTIONS ORDER BY 1",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
(param('dataset')) ? &displayErrors() : &displayQuery();
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


sub displayQuery
{
  &printHeader();
  $sth{DATASETS}->execute();
  my $ar = $sth{DATASETS}->fetchall_arrayref();
  my %label = map { $_->[0] => (sprintf '%s (%s error%s)',
                    $_->[0],$_->[1],((1 == $_->[1]) ? '' : 's'))} @$ar;
  my $count;
  $count += $_->[1] foreach (@$ar);
  $label{'All datasets'} = "All datasets ($count errors)";
  my $VERBIAGE = <<__EOT__;
This program will show the default 2D image for samples with an Error status for
a given dataset. You may select all datasets (this is the default), but it will
take some time to load all of the imagery.
__EOT__
  print div({class => 'boxed'},br,$VERBIAGE,br,br,
            'Select dataset to display errors for: ',
            popup_menu(&identify('dataset'),
                       -values => [sort keys %label],
                       -labels => \%label),br,
            div({align => 'center'},
                submit({&identify('submit'),
                        class => 'btn btn-success',
                        value => 'Submit'})));
  print end_form,&sessionFooter($Session),end_html;
}


sub displayErrors
{
  # Build HTML
  &printHeader();
  $sth{ERRORS}->execute();
  my $ar = $sth{ERRORS}->fetchall_arrayref();
  # Name, image
  my @row;
  foreach (@$ar) {
    push @row,[@$_];
    $row[-1][0] = a({href => "sample_search.cgi?sample_id=$row[-1][0]",
                     target => '_blank'},$row[-1][0]);
    (my $i = $row[-1][-1]) =~ s/.+filestore\///;
    if ($i) {
      $i = "/imagery_links/ws_imagery/$i";
      $row[-1][-1] = a({href => "http://jacs-webdav.int.janelia.org/WebDAV"
                                . $row[-1][-1],
                        target => '_blank'},
                            img({src => $i,
                                 width => '100'}));
    }
  }
  my @HEAD = ('Sample ID','Data set','Image');
  print "Errors: ",scalar @$ar,(NBSP)x5,
        &createExportFile($ar,"_ws_sample_errors",\@HEAD),
        table({id => 'details',class => 'tablesorter standard'},
              thead(Tr(th(\@HEAD))),
              tbody(map {Tr(td($_))} @row),
             );
  print end_form,&sessionFooter($Session),end_html;
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d%H:%M:%S",localtime)
                 . "$suffix.xls";
  $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  print $handle join("\t",@$_) . "\n" foreach (@$ar);
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
