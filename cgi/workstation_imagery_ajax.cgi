#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard/;
use JSON;
use LWP::Simple;
use Switch;
use URI::Encode qw(uri_decode);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/groups/scicompsoft/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
my (%CONFIG,%SERVER);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
my $TYPE = param('type') || 'mip';
my $ENTITY = param('entity') || 'lsm';
my $PRODUCT = param('product') || 'Signal MIP';
my $BACKUP = 'All Channel MIP';
my $HEIGHT = param('height') || 200;
my $COLOR = param('color') || '#ffffff';

# Get WS REST config
my $file = DATA_PATH . 'rest_services.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,my $slurp,-s SLURP;
close(SLURP);
my $hr = decode_json $slurp;
%CONFIG = %$hr;
$file = DATA_PATH . 'servers.json';
open SLURP,$file or &terminateProgram("Can't open $file: $!");
sysread SLURP,$slurp,-s SLURP;
close(SLURP);
$hr = decode_json $slurp;
%SERVER = %$hr;

my $response;
switch ($ENTITY) {
  case 'lsm'    { $response = &lsmMIP(); }
  case 'sample' { $response = &sampleMIP(); }
}
print header(-expires => 'now'),$response;
exit(0);

sub lsmMIP
{
  (my $name  = param('name')) =~ s/.*\///;
  my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{LSMImages} . "?name=$name";
  my $response = get $rest;
  &returnError("<h3>REST GET returned null response</h3>"
               . "<br>Request: $rest<br>") unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &returnError("<h3>REST GET failed</h3><br>Request: $rest<br>"
               . "Response: $response<br>Error: $@") if ($@);
  $PRODUCT = uri_decode($PRODUCT);
  my $img = $rvar->{files}{$PRODUCT} || $rvar->{files}{$BACKUP} || '';
  if ($img) {
    return(img({src => $SERVER{'jacs-storage'}{address} . $img,
                height => $HEIGHT}));
  }
  else {
    return("<div class='stamp'>No image found</div>");
  }
}


sub sampleMIP
{
  (my $id = param('id')) =~ s/.*\///;
  my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{SampleImage} . "?sampleId=$id&image=$PRODUCT";
  my $response = get $rest;
  &returnError("<h3>REST GET returned null response</h3>"
               . "<br>Request: $rest<br>") unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  &returnError("<h3>REST GET failed</h3><br>Request: $rest<br>"
               . "Response: $response<br>Error: $@") if ($@);
  if (scalar keys %$rvar) {
    my $table = table({class => 'detail'},
                      Tr(th('Sample name'),td($rvar->{name})),
                      Tr(th('TMOG date'),td($rvar->{tmogDate})),
                      Tr(th('Status'),td($rvar->{status})),
                      Tr(th('Data set'),td($rvar->{dataSet})),
                      Tr(th('Slide code'),td($rvar->{slideCode})),
                      Tr(th('Line'),td($rvar->{line})));
    my $html .= div({style => "float: left; border: 2px solid $COLOR"},
                    div({style => 'float:left'},
                        img({src => $SERVER{'jacs-storage'}{address} . $rvar->{image},
                             height => $HEIGHT})),
                    div({style => 'float:left'},$table))
                . div({style => 'clear: both;'},'');
    return($html);
  }
  else {
    return("<div class='stamp'>No sample found</div>");
  }
}


sub returnError
{
  print $_[0];
  exit(-1);
}
