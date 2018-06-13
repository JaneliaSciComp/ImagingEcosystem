#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Slide codes';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $FAMILY;

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
&terminateProgram('You are not authorized to run the Slide Code search app')
  unless ($Session->param('flylight_split_screen') || $Session->param('ptr_view') || $Session->param('scicomp'));
my $USERID = $Session->param('user_id');
$FAMILY = 'flylight*';
$FAMILY = 'projtechres' if ($Session->param('ptr_view'));
$FAMILY = 'all' if ($Session->param('scicomp'));
&render();
exit(0);


sub render
{
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('jquery/jquery.tablesorter',$PROGRAM);
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now'),
        start_multipart_form;
  print div({class => 'boxed'},
            div({align => 'center'},h2('Data set selection')),br,
            div({class => 'left'},
                div({class => 'left'},
                div({&identify('data_set_block')},''),
                div({&identify('slide_code_block')},''),
                div({&identify('objective_block')},''),
                ),
                div({&identify('display')},'')
               ),
            div({style => 'clear: both;'},NBSP),
            div({&identify('data_block')},''),
           );
  print hidden({&identify('family'),value => $FAMILY});
  print end_form,&sessionFooter($Session),end_html;
}
