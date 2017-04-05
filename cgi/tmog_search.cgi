#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use Switch;
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
our $APPLICATION = 'TMOG search';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
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
  EVENTS=> "SELECT * FROM logs WHERE datetime >= ? AND msg LIKE ? ORDER BY seq",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
if (param('image')) {
  &initializeProgram();
  &displayResults();
}
else {
  &showQuery();
}
# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub showQuery
{
  &printHeader();
  print <<__EOT__;
<br>
When images are processed with TMOG, messages are written to a logging database.
This interface will search the log for the portion of the image name that you
enter below (it can be either the microscope file name or the TMOGged filename).
The TMOG date is optional but <i>highly</i> recommeded to speed up the search.
__EOT__
  print button(-value => 'Sample data',
               -class => 'smallbutton',
               -onclick => 'populate();'),br,br;
  print table(Tr(td('Portion of image name',
                    input({&identify('image'),size => 40}))),
              Tr(td('TMOG date',
                    input({&identify('date'),type => 'text',size => 10}))),
             ),
        br,
        div({align => 'center'},
            submit({class => 'submitbutton'})),
        end_form,
        &sessionFooter($Session),end_html;
}


sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'syslog');
  $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    foreach (keys %sth);
}


sub displayResults
{
  &printHeader();
  my $date = param('date') || '2007-01-01';
  my $image = param('image');
  $sth{EVENTS}->execute($date,"%$image%");
  my $ar = $sth{EVENTS}->fetchall_arrayref();
  my $events = '';
  if (scalar @$ar) {
    foreach (@$ar) {
      foreach my $c (2..3) {
        switch ($_->[$c]) {
          case 'info'    { $_->[$c] = span({style => 'color: #66c'},$_->[$c]) }
          case 'warning' { $_->[$c] = span({style => 'color: #f90'},$_->[$c]) }
          case 'err'     { $_->[$c] = span({style => 'color: #c33'},$_->[$c]) }
        }
      }
    }
    $events = table({id => 'events',class => 'standard'},
                    thead(Tr(th([qw(Host Facility Priority Level
                                 Tag Date Program Message Seq)]))),
                    tbody(map {Tr(td($_))} @$ar));
  }
  # Render
  print div({align => 'center'},h2($image)),br,
        $events,
        div({style => 'clear: both;'},NBSP);
  print end_form,&sessionFooter($Session),end_html;
}


sub printHeader {
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ($PROGRAM);
  push @scripts,{-language=>'JavaScript',-src=>"https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"};
  my @styles = (Link({-rel=>'stylesheet',
                      -type=>'text/css',-href=>"https://code.jquery.com/ui/1.12.1/themes/cupertino/jquery-ui.css"}));
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now'),
        start_multipart_form;
}
