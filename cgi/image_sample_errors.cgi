#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use IO::File;
use JSON;
use LWP::Simple;
use POSIX qw(strftime);
use XML::Simple;
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/groups/scicompsoft/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Image sample errors';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';
my $BASE = "/var/www/html/output/";
my (%CONFIG,%SERVER);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Export
my $handle;
# Web
our ($USERID,$USERNAME);
my $Session;

# ****************************************************************************
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
my $AUTHORIZED = ($Session->param('scicomp'))
   || ($Session->param('workstation_flylight'));
&terminateProgram('You are not authorized to view Workstation imagery')
  unless ($AUTHORIZED);
our $DATASET = param('dataset') || '';
$DATASET = '' if ('All datasets' eq $DATASET);
my $OPTIONS = ($DATASET) ? "WHERE edd.value='$DATASET'" : '';


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
(param('dataset')) ? &displayErrors() : &displayQuery();
# We're done!
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Get WS REST config
  my $file = DATA_PATH . 'rest_services.json';
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
}


sub displayQuery
{
  &printHeader();
  my $ar;
    my $rest = $CONFIG{'jacs'}{url}.$CONFIG{'jacs'}{query}{SampleErrors};
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    push @$ar,[@{$_}{qw(dataSet count)}]
      foreach (sort {$a->{dataSet} cmp $b->{dataSet}} @$rvar);
  my %label = map { $_->[0] => (sprintf '%s (%s error%s)',
                    $_->[0],$_->[1],((1 == $_->[1]) ? '' : 's'))} @$ar;
  my $count;
  $count += $_->[1] foreach (@$ar);
  $label{'All datasets'} = "All datasets ($count errors)";
  my $VERBIAGE = 'This program will show the '
                 . popup_menu(&identify('image'),
                              -values => ['Default 2D Image','All MIP Image',
                                          'Reference MIP Image','Signal MIP Image']);
  $VERBIAGE .= <<__EOT__;
 for samples with an Error status for a given dataset. You may select
all datasets (this is the default), but it will take some time to
load all of the imagery.
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
  print hidden(&identify('mongo'),default=>param('mongo')),
        end_form,&sessionFooter($Session),end_html;
}


sub displayErrors
{
  # Build HTML
  &printHeader();
  my $ar;
    my $rest = $CONFIG{'jacs'}{url}.$CONFIG{'jacs'}{query}{ErrorMIPs};
    $rest .= "?dataset=$DATASET" if ($DATASET);
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                      . "Response: $response<br>Error: $@") if ($@);
    foreach (sort {$a->{sampleName} cmp $b->{sampleName}} @$rvar) {
        push @$ar,[@{$_}{qw(sampleName dataSet)},$_->{image}{'Signal MIP'}];
    }
  # Name, dataset, image
  my @row;
  foreach (@$ar) {
    push @row,[@$_];
    $row[-1][0] = a({href => "sample_search.cgi?sample_id=$row[-1][0]",
                     target => '_blank'},$row[-1][0]);
    if ($row[-1][-1]) {
      my $url = join('/',$SERVER{'jacs-storage'}{address},$row[-1][-1]);
      $row[-1][-1] = a({href => $url,
                        target => '_blank'},
                            img({src => $url,
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
