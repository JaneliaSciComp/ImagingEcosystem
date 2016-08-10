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

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our ($dbh);

# ****************************************************************************
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
my %sth = (
  PUBLISHED => "SELECT published_to,alps_release,COUNT(DISTINCT line),"
               . "COUNT(1) FROM image_data_mv WHERE published IS NOT NULL "
               . "GROUP BY 1,2",
  WAITING => "SELECT published_to,alps_release,publishing_user,"
             . "COUNT(DISTINCT line),COUNT(1) FROM image_data_mv WHERE "
             . "published IS NULL AND to_publish='Y' GROUP BY 1,2,3",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayDashboard();
# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'sage');
  $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    foreach (keys %sth);
}


sub displayDashboard
{
  &printHeader();
  my $service = JFRC::LDAP->new();
  my $annotator;
  # Waiting
  $sth{WAITING}->execute();
  my $ar = $sth{WAITING}->fetchall_arrayref();
  my $waiting;
  if (scalar @$ar) {
    foreach (@$ar) {
      my $u = $service->getUser($_->[2]);
      $annotator = join(' ',$u->{givenName},$u->{sn});
      $_->[2] = $annotator || $_->[2];
    }
    $waiting = table({id => 'waiting',class => 'tablesorter standard'},
                     thead(Tr(td(['Website','ALPS release','Annotator','Lines',
                                  'Images']))),
                     tbody(map {Tr(td($_))} @$ar));
  }
  # Published
  $sth{PUBLISHED}->execute();
  my $ar = $sth{PUBLISHED}->fetchall_arrayref();
  my $published;
  if (scalar @$ar) {
    $published = table({id => 'published',class => 'tablesorter standard'},
                       thead(Tr(td(['Website','ALPS release','Lines','Images']))),
                       tbody(map {Tr(td($_))} @$ar));
  }
  # Render
  print div({class => 'panel panel-warning'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},
                     'Awaiting publishing')),
            div({class => 'panel-body'},$waiting)),
        div({style => 'clear: both;'},NBSP) if ($waiting);
  print div({class => 'panel panel-success'},
            div({class => 'panel-heading'},
                span({class => 'panel-heading;'},
                     'Published')),
            div({class => 'panel-body'},$published)),
        div({style => 'clear: both;'},NBSP) if ($waiting);
  print end_form,&sessionFooter($Session),end_html;
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
