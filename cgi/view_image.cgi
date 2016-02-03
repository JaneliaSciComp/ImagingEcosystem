#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;

use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'View image';
use constant NBSP => '&nbsp;';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

my $content;

if (my $url = param('url')) {
  $content = &getContent($url);
}

  my @scripts = map { {-language=>'JavaScript',-src=>"/js/jquery/$_.js"} }
                      (qw(jquery-latest jquery-ui-latest));
  push @scripts,{-language=>'JavaScript',-src=>'/js/'.$PROGRAM.'.js'};
  print header,
        start_html(-title => $APPLICATION,
                   -style => {src => '/css/view_image.css'},
                   -script => \@scripts),
        body,$content,end_html;


sub getContent
{
  my $url = shift;
  (my $path = $url) =~ s/.*WebDAV//;
  return("$url was not found") unless (-r $path);
  my($content,$fp,$style) = ('')x3;
  my %value;
  my @attribute = qw(brightness contrast grayscale hue-rotate);
  foreach my $att (@attribute) {
    if (length(param($att))) {
      my $val = param($att);
      if ($val =~ /%/) {
        $val =~ s/%//;
      }
      elsif ($att !~ /hue/) {
        $val *= 100;
      }
      $val = sprintf '%.2f',$val unless ($att =~ /hue/);
      $value{$att} = $val;
      if ($att =~ /hue/) {
        $val .= 'deg';
        $fp .= "hue-rotate($val) ";
      }
      else {
        $val .= '%';
        $fp .= "$att($val) ";
      }
    }
  }
  $value{brightness} ||= 100;
  $value{contrast} ||= 100;
  $value{grayscale} ||= 0;
  $value{'huerotate'} ||= 0;
  $style = "filter: $fp; -webkit-filter: $fp" if ($fp);
  my @control = ();
  foreach (@attribute) {
    my $unit = (/hue/) ? '&deg;' : '%';
    $value{$_} ||= 0;
    my $max = 100;
    unless (/grayscale/) {
      $max = 200;
      if ($value{$_} > 200) {
        $max = (sprintf '%d',($value{$_}/100)+1)*100;
      }
    }
    $max = 360 if (/hue/);
    push @control,Tr(td([ucfirst($_) . ':',
                         '0'
                         . input({id => $_ . 'Slider',
                                  type => 'range',
                                  min => 0,
                                  max => $max,
                                  step => 1,
                                  value => $value{$_},
                                  onchange => "changeSlider('$_');"}) . $max
                                              . $unit,
                         input({id => $_,
                                type => 'text',
                                value => $value{$_},
                                size => 4,
                                onchange => "changeBox('$_');"}).$unit]));
  }
  (my $short = $url) =~ s/.*\///;
  $content .= div({class => 'imagebox'},
                 img({id => 'img',
                      style => $style,
                      src => $url}),br,
                 $short)
             . br . table({class => 'controls'},@control);
  return($content);
}
