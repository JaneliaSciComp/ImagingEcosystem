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

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
&terminateProgram('You are not authorized to run the Slide Code search app')
  unless ($Session->param('flylight_split_screen') || $Session->param('scicomp'));
my $USERID = $Session->param('user_id');
&render();
exit(0);


sub render
{
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ($PROGRAM);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now'),
        start_multipart_form;
  print div({class => 'boxed'},
            div({align => 'center'},h2('Data set selection')),br,
            div({class => 'main_block',style => 'float:left'},
                div({class => 'input_block',style => 'float:left'},
                div({id => 'data_set_block', name => 'data_set_block'},
                    img({src => '/images/loading.gif'}),'Loading data sets'
                   ),
                div({id => 'slide_code_block', name => 'slide_code_block'},''),
                div({id => 'objective_block', name => 'objective_block'},''),
                ),
                div({id => 'display', name => 'display', class => 'display',style => 'float:left'},'')
               ),
            div({style => 'clear: both;'},NBSP),
            div({id => 'data_block', name => 'data_block'},''),
           );
  print end_form,&sessionFooter($Session),end_html;
}
