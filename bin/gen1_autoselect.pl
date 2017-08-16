#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use JSON;
use LWP::Simple;
use POSIX qw(ceil);
use Switch;

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
my %CONFIG;


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
}


sub processLines
{
  my $rest = $CONFIG{sage}{url}.'gen1_images?family=dickson&line=BJD_1*';
  my $response = get $rest;
  &terminateProgram("REST GET returned null response (Request: $rest)")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  my (%line,%line_count,%pie,%pw);
  my $selected = 0;
  foreach (@{$rvar->{image_data}}) {
    my($line,$driver,$area,$annotator,$vtid,$vtmatch) = @$_{qw(line driver area annotator vt_id_found vt_library_match)};
    next if ($driver ne 'GAL4_Collection');
    my($short) = $line =~ /(BJD_\d{3}.{3})/;
    my $vt;
    if ($vtmatch eq 'Y') {
      $vt = 'VT match';
    }
    elsif ($vtid eq 'Y') {
      $vt = 'VT mismatch';
    }
    else {
      $vt = 'No VT';
    }
    $_->{vt_status} = $vt;
    push @{$line{$short}{$area}},$_;
    $annotator ||= '';
    $line{$short}{Annotated}{$area}++ if ($annotator);
  }
  foreach my $sl (sort keys %line) {
    print "$sl\n";
    # Skip if we don't have a brain
    unless (exists $line{$sl}{Brain}) {
      print "  Missing brain for $sl\n";
      next;
    }
    # Skip annotated lines
    if ($line{$sl}{Annotated}{Brain} && $line{$sl}{Annotated}{VNC}) {
      print "  Line $sl is already annotated\n";
      next;
    }
    # Choose a line/image
    if ((scalar @{$line{$sl}{Brain}} == 1) && ($line{$sl}{Brain}[0]{vt_status} ne 'VT mismatch')) {
      my $line = $line{$sl}{Brain}[0]{line};
      my $image = $line{$sl}{Brain}[0]{id};
      print "  Choose brain $line $image $line{$sl}{Brain}[0]{vt_status}\n";
      next;
    }
  }
}


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&processLines();
