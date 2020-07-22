#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;
use DBI;
use JSON;
use LWP::Simple qw(get);
use Time::HiRes qw(gettimeofday tv_interval);
use XML::Simple;

use JFRC::Utils::Web qw(:all);
use JFRC::Utils::SAGE qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
use constant DATA_PATH  => '/groups/scicompsoft/informatics/data/';
# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Line manager';
my $DB = 'dbi:mysql:dbname=sage;host=';
my $DBF = 'dbi:mysql:dbname=flyboy;host=prd-db';
my $DBL = 'dbi:mysql:dbname=sage;host=';
my $DBW = 'dbi:mysql:dbname=wip;host=mysql2';
my $DBWS = 'dbi:mysql:dbname=flyportal;host=prd-db';
use constant NBSP => '&nbsp;';
my @BREADCRUMBS = ('Database tools',
                   'http://informatics-prod.int.janelia.org/#databases');
# Tab order
my %TAB = (search => 0,
           add => 1);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Parameters
my ($DATABASE,$OPERATOR);
# Database
my %sth = (
ARENA => "SELECT DISTINCT 'Fly Olympiad aggression',a.name,ep.value FROM "
         . 'experiment_vw e JOIN experiment_relationship er ON '
         . '(er.subject_id=e.id) JOIN experiment a ON (a.id=er.object_id) JOIN '
         . 'session s ON (e.id=s.experiment_id) JOIN line l ON (s.line_id=l.id)'
         . ' JOIN experiment_property_vw ep ON (ep.experiment_id=a.id AND '
         . "ep.type='exp_datetime') WHERE e.cv='fly_olympiad_aggression' AND "
         . 'l.name=?',
EXP => 'SELECT display_name,e.name,ep.value FROM experiment_vw e '
       . 'JOIN session_vw s ON (e.id=s.experiment_id AND s.line=?) '
       . 'JOIN cv ON (cv.name=e.cv) JOIN experiment_property_vw ep ON '
       . "(e.id=ep.experiment_id AND ep.type='exp_datetime') GROUP BY 1,2",
GENE => 'SELECT gene from image_data_vw WHERE line=?',
IBL => 'SELECT i.family,i.age,i.driver,i.imaging_project,i.effector,'
       . 'i.data_set,si.url,si.product FROM image_data_mv i '
       . 'JOIN secondary_image_vw si ON (i.id=si.image_id) '
       . "WHERE i.family NOT LIKE 'fly_olympiad%' AND i.line=? ORDER BY 1,2,7,8",
PRI => 'SELECT i.family,d.product,i.path,i.url FROM image_vw i '
       . 'JOIN image_data_mv d ON (i.id=d.id)  WHERE i.line=? AND '
       . "(i.family LIKE 'fly_olympiad%' OR i.family like '%external')",
IMAGERY => 'SELECT family,COUNT(1) FROM image_data_mv WHERE line=? GROUP BY 1',
IMAGES => 'SELECT i.family,i.name,MAX(s.url) FROM image_data_mv i LEFT JOIN '
          . 'secondary_image_vw s ON (i.id=s.image_id AND '
          . "s.product IN ('projection_all','projection_green',"
          . "'projection_pattern','multichannel_mip','signal1_mip')) WHERE line=? AND "
          . "family NOT LIKE '%olympiad%' AND family != 'simpson_lab_grooming' "
          . 'GROUP BY 1,2',
IMAGEU => 'SELECT url FROM image WHERE name=?',
LAB => "SELECT cv_term,definition FROM cv_term_vw WHERE cv='lab'",
LINE => 'SELECT name,id,lab_display_name,organism,create_date FROM line_vw WHERE name=? OR genotype=? OR robot_id=? OR id IN (SELECT line_id FROM publishing_name WHERE publishing_name=?)',
LINES => "SELECT DISTINCT name FROM line UNION SELECT DISTINCT publishing_name FROM publishing_name WHERE published=1 OR (for_publishing=1 AND label=0) ORDER BY 1",
LINEPROP => 'SELECT getCVTermDisplayName(getCVTermID(cv,type,NULL)) AS type,'
            . 'getCVTermDefinition(getCVTermID(cv,type,NULL)) AS definition,'
            . 'value FROM line_property_vw WHERE name=? ORDER BY 1',
LINEREL => "SELECT relationship,object,value FROM line_relationship_vw lr LEFT OUTER JOIN line_property_vw lp ON (lp.name=object AND lp.type='flycore_project') WHERE "
           . 'subject=? ORDER BY 2,1',
PREFIX => "SELECT cv_term,definition FROM cv_term_vw WHERE cv='lab_prefix'",
PUBLISHING => "SELECT publishing_name,requester,IF(published,'Yes','No'),IF(label,'Yes','No') FROM publishing_name_vw WHERE line=? ORDER BY 1",
PUBLISHED => "SELECT publishing_name,alps_release,COUNT(1) FROM image_data_mv WHERE line=? AND to_publish='Y' GROUP BY 1,2",
SPECIES => "SELECT id,CONCAT(genus,' ',species) AS name FROM organism",
);
my %sthf = (
FLYSTORE => 'SELECT date_submitted,ordered_for,status,date_filled FROM '
            . 'FlyStore_line_order_history_vw WHERE stock_name=? ORDER BY 1',
SH => 'SELECT event_date,event,stock_name,cross_stock_name2,cross_effector,'
      . 'project,lab_project,cross_type,cross_barcode,wish_list FROM '
      . 'stock_event_history_vw WHERE stock_name=? OR cross_stock_name2=? ORDER BY event_date,event',
);
my %sthl = (
EXP => 'SELECT e.name,COUNT(s.name) FROM experiment_vw e JOIN session_vw s ON '
       . '(e.id=s.experiment_id AND s.line=?) JOIN cv ON (cv.name=e.cv) '
       . 'GROUP BY 1',
);
my %sthw = (
ACTIVE => 'SELECT process,action,create_date FROM active_line_vw WHERE name=? ORDER BY create_date DESC',
BATCHESL => 'SELECT b.name FROM line l JOIN line_batch_history_vw v ON '
            . '(l.id=line_id) JOIN batch b ON (b.id=v.batch_id) WHERE '
            . 'l.name=? ORDER BY 1',
);
my %sthws = (
IMG => "SELECT ss.name sampleName, absd2di.value mip,d2di.value heatmapMip FROM entity ss LEFT OUTER JOIN entityData d2di ON (ss.id=d2di.parent_entity_id AND d2di.entity_att='Default 2D Image File Path') LEFT OUTER JOIN entityData ssc ON (ss.id=ssc.parent_entity_id) LEFT OUTER JOIN entity abs ON (ssc.child_entity_id=abs.id AND abs.entity_type='Aligned Brain Stack') JOIN entityData absd2di ON (abs.id=absd2di.parent_entity_id) WHERE absd2di.entity_att='Default 2D Image File Path' AND ss.name LIKE ?",
SAMPLE => "SELECT distinct ss.name sampleName,absd2di.value mip FROM entity ss LEFT OUTER JOIN entityData ssc ON (ss.id=ssc.parent_entity_id) LEFT OUTER JOIN entity abs ON (ssc.child_entity_id=abs.id AND abs.entity_type='Sample') JOIN entityData absd2di ON (abs.id=absd2di.parent_entity_id) WHERE absd2di.entity_att IN ('Default 2D Image File Path','Signal MIP Image','Reference MIP Image') AND ss.name LIKE ?"
);
our ($dbh,$dbhf,$dbhl,$dbhw,$dbhws);
# XML configuration
my (%LINK_MAP,%TERM);
my $js_file = "";
# Configuration
my %SERVER;
# General
my $height = 60;
my $CLASS;

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

# Session authentication
my $Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$OPERATOR = $Session->param('user_name');
my $UID = $Session->param('user_id');
my $ADD = $Session->param('sage_line_add');
my $VIEW = $Session->param('sage_line_view');
my $SCICOMP = ($Session->param('scicomp'));

&initializeProgram();
if (param('Search')) {
  &displaySummary();
}
elsif ((param('mode') eq 'ibl') && param('line')) {
  &displayImagesByLine(param('line'));
}
elsif (param('line')) {
  &displayLine(param('line'));
}
else {
  &showQuery();
}

# We're done!
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to SAGE database
  $DATABASE = lc(param('_database') || 'prod');
  $DB .= ($DATABASE eq 'prod') ? 'mysql3'
                               : (($DATABASE eq 'val') ? 'val-db' : 'dev-db');
  eval {
    $dbh = DBI->connect($DB,('sageRead')x2,{RaiseError=>1,PrintError=>0});
  };
  &terminateProgram('Could not connect to SAGE database:<br>'.$@) if ($@);
  $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
    foreach (keys %sth);
  # Connect to FlyBoy database
  eval {
    $dbhf = DBI->connect($DBF,('flyfRead')x2,{RaiseError=>1,PrintError=>0});
  };
  &terminateProgram('Could not connect to FlyBoy database:<br>'.$@)
    if ($@);
  $sthf{$_} = $dbhf->prepare($sthf{$_}) || &terminateProgram($dbhf->errstr)
    foreach (keys %sthf);
  # Connect to WIP database
  eval {
    $dbhw = DBI->connect($DBW,('wipRead')x2,{RaiseError=>1,PrintError=>0});
  };
  &terminateProgram('Could not connect to WIP database:<br>'.$@)
    if ($@);
  $sthw{$_} = $dbhw->prepare($sthw{$_}) || &terminateProgram($dbhw->errstr)
    foreach (keys %sthw);
  # Connect to Janelia Workstation database
  eval {
    $dbhws = DBI->connect($DBWS,('flyportalRead')x2,{RaiseError=>1,PrintError=>0});
  };
  &terminateProgram('Could not connect to Janelia Workstation database:<br>'.$@)
    if ($@);
  $sthws{$_} = $dbhws->prepare($sthws{$_}) || &terminateProgram($dbhws->errstr)
    foreach (keys %sthws);
  # Connect to Larval SAGE database
  $DATABASE = lc(param('_database') || 'prod');
  $DBL .= ($DATABASE eq 'prod') ? 'larval-sage-db'
                                : (($DATABASE eq 'val') ? 'val-db' : 'dev-db');
  eval {
    $dbhl = DBI->connect($DBL,('sageRead')x2,{RaiseError=>1,PrintError=>0});
  };
  &terminateProgram('Could not connect to Larval SAGE database:<br>'.$@)
    if ($@);
  $sthl{$_} = $dbhl->prepare($sthl{$_}) || &terminateProgram($dbhl->errstr)
    foreach (keys %sthl);
  # Configure XML
  my $p;
  eval { 
    $p = XMLin(DATA_PATH . $PROGRAM . '-config.xml',
               KeyAttr => { term => 'key' },
              );
  };
  &terminateProgram("Could not configure from XML file: $@") if ($@);
  %TERM = %{$p->{term}};
  $LINK_MAP{$_->{site}} = $_->{content} foreach (@{$p->{link}});
  my $rest = 'http://config.int.janelia.org/config/servers';
  my $response = get $rest;
  my $rvar;
  eval {$rvar = decode_json($response)};
  &terminateProgram("<h3>REST GET failed</h3><br>Request: $rest<br>"
                    . "Response: $response<br>Error: $@") if ($@);
  %SERVER = %{$rvar->{config}};
}


sub showQuery
{
  # ----- Page header -----
  print &pageHead(),start_multipart_form,&hiddenParameters();
  my @content = ({id=>'main',title=>'Search',content=>&searchTab()});
  if ($ADD) {
    push @content,({id=>'add',title=>'Add',content=>&addTab()},
                   {id=>'batch_add',title=>'Batch upload',content=>&batchUploadTab()});
  }
  my($class,$class2) = ('active','tab-pane active');
  print div({role=>'tabpanel'},
            ul({class=>'nav nav-tabs',role=>'tablist'},
               map {$a = li({role=>'presentation',class=>$class},
                            a({href=>'#'.$_->{id},
                               'aria-controls'=>$_->{id},
                               role=>'tab',
                               'data-toggle'=>'tab'},$_->{title}));
                    $class = '';
                    $a;
                   } @content),br,
            div({class=>'tab-content'},
                map {$a = div({role=>'tabpanel',class=>$class2,id=>$_->{id}},
                              $_->{content});
                     $class2 = 'tab-pane';
                     $a} @content),
           );
  # ----- Footer -----
  print end_form,&sessionFooter($Session),end_html;
}


sub searchTab
{
  $sth{LINES}->execute;
  my $ar = $sth{LINES}->fetchall_arrayref();
  my @lines;
  push @lines,$_->[0] foreach (@$ar);
   h3('Search for a line') . br
   . div({style => 'float: left;'},
         'Line: ' . input({&identify('lines')}) . br
         . scrolling_list({&identify('line'),
                           values => \@lines,
                           size => 10,
                           multiple => 'true'}) . br
         . div({class => 'formalert'},''))
   . div({style => 'float: left;'},
         'Publication name (Genotype): ' . input({&identify('genotype')}))
   . div({style => 'clear:both;',
         align => 'center'},submit({&identify('Search'),class => 'submitbutton',value => 'Search'}));
}


sub addTab
{
  $sth{LAB}->execute();
  my $hr = $sth{LAB}->fetchall_hashref('cv_term');
  my %lab = map { $_ => $hr->{$_}{definition} } (keys %$hr);
  ($lab{$_} =~ /Lab$/) || delete($lab{$_}) foreach (keys %lab);
  delete $lab{'test-lab'};
  $sth{PREFIX}->execute();
  $hr = $sth{PREFIX}->fetchall_hashref('cv_term');
  my %prefix = map { $_ => $hr->{$_}{definition} } (keys %$hr);
  $prefix{''} = '(none)';
  $sth{SPECIES}->execute();
  $hr = $sth{SPECIES}->fetchall_hashref('id');
  my %species = map { $_ => $hr->{$_}{name} } (keys %$hr);
  my $addbutton = button(-value => 'Add',-onClick => 'addLine();');
  my($default_prefix,$default_lab) = ('FCF','flylight');
  ($default_prefix,$default_lab) = ('EDH','anderson')
    if ('hoopfere' eq $UID);
  h3('Add a line')
  . div({class => 'parents'},
        div({class => 'parentline'},
            p('Parent line '.span({style => 'font-size: 8pt;'},'(optional)')),
            input({&identify('line1'),-size => 25,
                   -onChange => "blankDiv('line1','line1m')"}),
            div({&identify('line1m'),class => 'line_metadata'},''),
        ),
        div({class => 'parentline'},
            p('Parent line '.span({style => 'font-size: 8pt;'},'(optional)')),
            input({&identify('line2'),-size => 25,
                   -onChange => "blankDiv('line2','line2m')"}),
            div({&identify('line2m'),class => 'line_metadata'},''),
        )
       ) # end parents block
  . div({class => 'childline'},
    table(
          Tr(td(['Line: ',
                 div({},
                     div({&identify('lineprefix'),
                          style => 'float: left;'},$default_prefix.'_')
                     . input({&identify('aline')})
                 )])),
          Tr(td(['Line prefix: ',
                 popup_menu(&identify('prefix'),
                            -default => $default_prefix,
                            -values => [sort(keys %prefix)],
                            -labels => \%prefix,
                            -onChange => 'setPrefix();')])),
          Tr(td(['Lab: ',
                 popup_menu(&identify('lab'),
                            -default => $default_lab,
                            -values => [sort(keys %lab)],
                            -labels => \%lab)])),
          Tr({style => 'display: none;'},td(['Species: ',
                 popup_menu(&identify('organism'),
                            -values => [sort(keys %species)],
                            -labels => \%species)])),
          Tr(td(['Publication name (Genotype): ',
                 input({&identify('agenotype')})
                 . NBSP.span({style => 'font-size: 8pt;'},'(required)')])),
          Tr(td(['Gene: ',
                 input({&identify('agene')})
                 . NBSP.span({style => 'font-size: 8pt;'},'(optional)')])),
          Tr(td(['Description: ',
                 input({&identify('description')})
                 . NBSP.span({style => 'font-size: 8pt;'},'(optional)')])),
          Tr(td({colspan => 2,align => 'center'},'Line is known to Fly Core: '
                . radio_group(&identify('flycore_known'),
                             -values => ['Yes','No'],
                             -default => 'Yes',
                             -onClick => 'flycoreToggle();') .br
                . button(&identify('fcbutton'),
                         -value => 'Populate from Fly Core using line',
                         -class => 'smallbutton',
                         -onClick => 'autofillLine();'))),
          Tr(td({class => 'fc'},['Fly Core ID '
                . span({style => 'font-size: 8pt;'},'(__kp_UniqueID)').': ',
                input({&identify('flycoreid')})
                . NBSP.span({style => 'font-size: 8pt;'},'(optional)')])),
          Tr(td({class => 'fc'},['Robot ID: ',
                 input({&identify('robotid')})
                 . NBSP.span({style => 'font-size: 8pt;'},'(optional)')])),
         ))
  . div({style => 'clear:both;',
         align => 'center'},$addbutton);
}


sub batchUploadTab
{
  if (param('line_file')){
    my $input_file = param('line_file');
    my @input_row = &convert_input($input_file);
    &add_lines(@input_row);
  }
  print $js_file;
  h3('Batch upload lines')
  . br
  . div("Line batch upload input consists of nine tab-delimited columns",ol(li("Parent 1 (optional)"), li("Parent 2 (optional)"), li("Line (required)"), li("Lab (required)"), li("Genotype (required)"), li("Gene (optional)"), li("Description (optional)"), li("Flycore ID (required)"), li("Robot ID (required)")))
  . label({for=>'line_file'}, 'Upload File: ')
  . filefield({size=>40,name=>'line_file', id=>'line_file'})
  . br
  . input({type=>'submit', class => 'submitbutton', value=>'Submit'})
  . end_form;
}


sub add_lines
{
  my @lines_to_add = @_;
  shift(@lines_to_add);
  $js_file .= qq~
  <script type='text/javascript'>
  function load_lines(){
  var lines_to_add = ~;
  $js_file .= "'";
  my $i_ar = join("|", @lines_to_add);
  $js_file .= $i_ar;
  $js_file .= "';";
  $js_file .= qq~
           \$.ajax({type:'POST',
             async: false,
             url: '../sage_ajax.php',
             data: {
               query:'batch_upload_lines',
               lines:lines_to_add,
               operator:'$OPERATOR',
               db:'mysql3'
             },
             success: function(data){
               alert(data);
             },
             error: function(data){
               alert(data);
             }

           });
           }</script>
           ~;
}


# ****************************************************************************
# * Subroutine:  convert_input                                               *
# * Description: This routine will create an array of rows from the input    *
# *              stream. This may seem like a waste of time, but trust me:   *
# *              it ain't. We should be able to handle any common IFS, and   *
# *              the expected input will be from Excel running on a Mac.     *
# *              Despite the fact that Excel is running on OS X (Unix-style  *
# *              line separators), it still produces the old-school Mac OFS. *
# *                                                                          *
# * Parameters:  stream: file handle to process                              *
# * Returns:     @field: array of input rows                                 *
# ****************************************************************************
sub convert_input
{
  my $stream = shift;
  my $buffer = '';
  $buffer .= $_ while (<$stream>);

  # Windows: CR/LF
  my @field = split(/\015\012/,$buffer);
  &terminateProgram('Could not process import file') unless (scalar @field);
  if (1 == scalar(@field)) {
    # Mac (pre-OS X): CR
    @field = split(/\012/,$buffer);
    if (1 == scalar(@field)) {
      # Unix: LF
      @field = split(/\015/,$buffer);
      if (1 == scalar(@field)) {
        &terminateProgram('Unknown input format');
      }
    }
  }
  return(@field);
}


sub displaySummary
{
  my $ar = &executeQuery(CONDITION => \my %condition,
                         TYPE => 'summary',
                         SEARCH => 'general',
                         LOGIC => 'OR',
                         TERM => \%TERM);
  # Short circuit if there's only one line
  if (1 == scalar(@$ar)) {
    &displayLine($ar->[0][0]);
    return;
  }
  foreach (@$ar) {
    my $l = $_->[0];
    my $syn = $_->[2];
    if ($syn) {
      # Replace gene
      my @g = split(', ',$syn);
      my $lid = $l . '_syn';
      $_->[1] = a({id => $lid . 'l',
                  href => '#'.$lid,
                  title => 'Show gene synonyms',
                  onClick => "toggleSynonyms('$lid'); return false;"},
                img({id => $lid . 'i',
                     src => '/css/plus.gif'})) . $_->[1]
                . div({id => $lid,
                       style => 'display: none;'},
                      join(', ',@g));
    }
    splice(@$_,2,1);
    if ($_->[-1]) {
      # Add links
      my @sess = split(/\s*,\s*/,$_->[-1]);
      my @new_sess;
      foreach (@sess) {
        my($t,$num) = $_ =~ /(.+)\s+(\(\d+\))/;
        $t = a({href   => $LINK_MAP{$t} . (($t =~ /lethality/) ? '' : $l),
                title  => 'Open experiment data in a new window',
                target => '_blank'},$t) if (exists $LINK_MAP{$t});
        push @new_sess,"$t $num";
      }
      @new_sess = (NBSP) unless (scalar @new_sess);
      $_->[-1] = join(', ',@new_sess);
    }
    $sth{IMAGERY}->execute($_->[0]);
    my $ar2 = $sth{IMAGERY}->fetchall_arrayref();
    if (scalar @$ar2) {
      my %ihash;
      foreach (@$ar2) {
        unless ($_->[0] =~ s/^fly_olympiad_.+/Fly Olympiad/) {
          $_->[0] =~ s/_.+//;
        }
        $ihash{ucfirst($_->[0])} += $_->[1];
      }
      push @$_,join(', ',map {sprintf '%s (%d)',$_,$ihash{$_}} sort keys %ihash);
    }
    else {
      push @$_,NBSP;
    }
  }
  # ----- Page header -----
  print &pageHead(mode => 'summary'),start_form,&hiddenParameters();
  # ----- Contents -----
  my @header = qw(Line Gene Lab Genotype Sessions Images);
  my %advanced = (QUERY => delete $condition{QUERY});
  delete @condition{qw(agene aline)};
  foreach (@$ar) {
    $_->[0] = a({href => '?line=' . $_->[0]},$_->[0]);
  }
  print div({style => 'width: 875px;'},
            # Search conditions
            div({style => 'padding-left: 15px;'},
                a({id  => 'clink',
                   href => '#',
                   title => 'Show search conditions',
                   onClick => 'toggleConditions();'},
                  img({id => 'clinki',
                       src => '/css/plus.gif'})),
                (sprintf 'Found %d line%s.',scalar(@$ar),
                                      (1 == scalar(@$ar)) ? '' : 's')),
            div({style => 'float: left;'},
                table({id => 'conditions',
                       style => 'risplay: none;'}, #PLUG
                      map {Tr(td([$TERM{$_}{display}||$_,
                                  join(', ',@{$condition{$_}})]))}
                          sort(keys %condition)),
                table({id => 'advanced',
                       style => 'risplay: none;'},
                      map {Tr(td([$TERM{$_}{display}||$_,
                                  join(', ',@{$advanced{$_}})]))}
                          sort(keys %advanced))),
            div({style => 'clear: both;'},br),
            # Summary table
            ((scalar @$ar)
             ? table({class => 'sortable',id => 'linelist'},
                     Tr(th(\@header)),
                     map {
                       my $i = pop @$_;
                       Tr(td($_),td({style => 'width: 310px;'},$i));
                     } @$ar)
             : (NBSP)x5 . a({href => '?'},'Search again'))
           );

  # ----- Footer -----
  print end_form,&sessionFooter($Session),end_html;
}


sub displayImagesByLine
{
  my $line = shift;
  # ----- Page header -----
  print &pageHead(mode => 'detail',title => $line),
        start_form,&hiddenParameters();
  my %imagery;
  $sth{IBL}->execute($line);
  my $ar = $sth{IBL}->fetchall_arrayref();
  $height = param('height') || 60;
  my %product;
  my %image_count;
  # Secondary data
  if (scalar @$ar) {
    foreach (@$ar) {
      my($family,$age,$driver,$ip,$reporter,$dataset,$url,$product) = @$_;
      next unless ($url =~ /(?:avi|jpg|png|mov|mp4)$/);
      $product{$product}++;
      my $img = &imageryLink($url,$url);
      my @label;
      foreach ($age,$driver,$ip,$reporter,$dataset) {
        push @label,$_ if ($_);
      }
      push @label,$product;
      $img .= div({class => 'lbl'},
                  div({style => 'font-size: 10px;'},join(br,@label)));
      $imagery{$family} .= div({class => 'labeled_img '.$product},$img);
      $image_count{ALL}++;
      $image_count{$product}++;
    }
  }
  # Primary imagery
  $sth{PRI}->execute($line);
  $ar = $sth{PRI}->fetchall_arrayref();
  foreach (@$ar) {
    my($family,$product_detail,$path,$url) = @$_;
    next unless ($url =~ /(?:avi|jpg|png|mov|mp4)$/);
    next unless (-r $path);
    (my $product = $family) =~ s/fly_olympiad_//;
    if ($family =~ /fly_olympiad/) {
      $family = 'Fly Olympiad';
      $product = ucfirst($product);
      $product_detail =~ s/^seq\d+_//;
      $product_detail =~ s/_/ /g;
      $product_detail = $product . br . $product_detail;
    }
    else {
      $product_detail = $product;
    }
    $product{$product}++;
    my $img = &imageryLink($url,$path);
    $img .= div({class => 'lbl'},
                div({style => 'font-size: 10px;'},$product_detail));
    $imagery{$family} .= div({class => 'labeled_img '.$product},$img);
    $image_count{ALL}++;
    $image_count{$product}++;
  }
  # Janelia Workstation imagery
  if ($SCICOMP) {
    my $t0 = [gettimeofday];
    $sthws{IMG}->execute($line.'%');
    $ar = $sthws{IMG}->fetchall_arrayref();
    printf STDERR "  IMG query: %.3f sec\n",tv_interval($t0);
    if (scalar @$ar) {
      $product{$_}++ foreach (qw(workstation_mip workstation_heatmap));
      foreach (@$ar) {
        my($samplename,$mip,$heatmap) = @$_;
        if ($mip) {
          $imagery{'Janelia Workstation'}
            .= div({class => 'labeled_img workstation_mip'},
                   &workstationImage($mip,$height,'MIP',2));
          $image_count{ALL}++;
          $image_count{workstation_mip}++;
        }
        if ($heatmap) {
          $imagery{'Janelia Workstation'}
            .= div({class => 'labeled_img workstation_heatmap'},
                   &workstationImage($heatmap,$height,'Heatmap'));
          $image_count{ALL}++;
          $image_count{workstation_heatmap}++;
        }
      }
    }
    &addWorkstationImagery('SAMPLE','workstation_sample_mip','Sample MIP',$line,\%product,\%imagery,\%image_count);
  }
  # Render
  if ($image_count{ALL}) {
    print h2("Imagery for $line"),br,
          'Images: ',$image_count{ALL},br,
          checkbox(&identify('showlbl'),
                   -label => 'Show image labels',
                   -checked => 1,
                   -onClick => "toggleClass('lbl');");
    # Product checkboxes
    my @pcb;
    foreach (sort keys %product) {
      push @pcb,checkbox(&identify('show'.$_),
                         -label => "Show $_ images ($image_count{$_})",
                         -checked => 1,
                         -onClick => "toggleClass('$_');");
    }
    print br,div({style => 'border: 1px solid cyan;'},join(br,@pcb)),br;
    foreach (sort keys %imagery) {
      print div({class => 'img_container'},h3($_),div($imagery{$_}));
    }
    print div({style=>'clear:both;'},NBSP);
  }
  # ----- Footer -----
  print end_form,&sessionFooter($Session),end_html;
}


sub addWorkstationImagery
{
  my($statement,$key,$caption,$line,$product,$imagery,$image_count) = @_;
  my $t0 = [gettimeofday];
  $sthws{$statement}->execute($line.'%');
  my $ar = $sthws{$statement}->fetchall_arrayref();
  printf STDERR "  SAMPLE query: %.3f sec\n",tv_interval($t0);
  if (scalar @$ar) {
    $product->{$key}++;
    foreach (@$ar) {
      my($samplename,$image) = @$_;
      $imagery->{'Janelia Workstation'}
        .= div({class => 'labeled_img '.$key},
               &workstationImage($image,$height,$caption));
      $image_count->{ALL}++;
      $image_count->{$key}++;
    }
  }
}


sub imageryLink
{
  my($url,$path) = @_;
  my $image = $url;
  my($ext) = $url =~ /\.([A-Za-z0-9]+)$/;
  $ext = lc($ext);
  unless ($ext =~ /g$/) {
    $image = '/images/movie_' . $ext . '.png';
  }
  my $suffix = '?proportional=yes&height='.$height;
  my %options = ();
  if ($url =~ /(?:WebDAV|jade)/i) {
    $suffix = '';
    %options = (height => 60);
  }
  a({href => $url,
     target => '_blank'},
    ($path =~ /g$/)
      ? img({src => $url . $suffix,%options})
      : img({src => $image, height => $height, width => $height})
   );
}


sub workstationImage
{
  my($path,$height,$caption,$ratio) = @_;
  $ratio ||= 2;
  my $img = img({src => $SERVER{'jacs-storage'}{address}.$path,
                 height => $height,
                 width => $height*$ratio});
  $img = a({href => $SERVER{'jacs-storage'}{address}.$path,
            target => '_blank'},$img);
  $img .= div({class => 'lbl'},
              div({style => 'font-size: 10px;'},$caption));
  return($img);
}


sub displayLine
{
  my $line = shift;
  # ----- Page header -----
  print &pageHead(mode => 'detail',title => $line),
        start_form,&hiddenParameters();

  my $lp;
  my %primaryprop;
  $sth{LINE}->execute(($line)x4);
  ($line,$primaryprop{'SAGE ID'},$primaryprop{Lab},$primaryprop{Organism},$primaryprop{'SAGE Create date'}) = $sth{LINE}->fetchrow_array;
  # ----- Line properties -----
  # Get gene information
  $sth{GENE}->execute($line);
  my $ar = $sth{GENE}->fetchall_arrayref;
  my %gene;
  my $cg;
  if (scalar @$ar) {
    ($_->[0] =~ /^CG\d+$/) ? ($cg = $_->[0]) : ($gene{$_->[0]}++) foreach (@$ar);
  }
  # Line properties
  $sth{LINEPROP}->execute($line);
  $lp = $sth{LINEPROP}->fetchall_hashref('type');
  $CLASS = $lp->{'Fly Core Permission'}{value} || '';
  foreach ('Fly Core Alias','Fly Core fragment') {
    $lp->{$_}{value} = span({class => 'redacted'},'HIDDEN')
    if ($CLASS =~ /3/ && !$VIEW);
  }
  $lp->{$_}{value} = $primaryprop{$_} foreach (keys %primaryprop);
  if (($lp->{Hide}{value} eq 'Y') && !$VIEW) {
    print div({&identify('summaryarea'),
               style => 'margin: 0 10px 0 10px;'},
              div({align => 'center'},h3($line),br,
                  img({src => '/images/redacted.png'}),br,
                  h3(span({class => 'redacted'},
                          'The data for this line is hidden')))),
          end_form,&sessionFooter($Session),end_html;
    return;
  }
  my $v = 'Primary publication (DOI)';
  $lp->{$v}{value} = a({href => 'https://dx.doi.org/'
                                . $lp->{$v}{value},
                        target => '_blank'},$lp->{$v}{value})
    if (exists $lp->{$v}{value});
  $lp->{'Fly Core ID'}{value} = a({href => 'http://informatics-prod.int.janelia.org/'
                                           . 'flyboy_search.php?kpid='
                                           . $lp->{'Fly Core ID'}{value},
                                   target => '_blank'},
                                  $lp->{'Fly Core ID'}{value})
    if (exists $lp->{'Fly Core ID'}{value});
  $lp->{Gene}{value} = join(', ',$cg,sort keys %gene) if ($cg);
  if ($lp->{Strand}{value}) {
    $lp->{Strand}{value} = '+' . $lp->{Strand}{value}
      if (1 == $lp->{Strand}{value});
  }
  else {
    delete($lp->{Strand});
  }
  if (exists $lp->{'Fly Core production info'}{value}) {
    $lp->{'Fly Core production info'}{value} =
      span({style => 'color: #ff4455;font-weight: bold;'},
           $lp->{'Fly Core production info'}{value})
      if ($lp->{'Fly Core production info'}{value} =~ /(?:Dead|GSI Fail|Tossed)/);
  }
  # Get publishing data
  my $pd = &getPublishing($line);
  # Get line relationships
  my $lr = ($VIEW) ? &getLineRel($line) : '';
  # Get genomic information
  my $gt = &getGenomic($lp);
  # Render
  my $display_line = $line;
  if ($lp->{Hide}{value} eq 'Y') {
    $display_line .= " (hidden)";
  }
  delete($lp->{Hide});
  print div({&identify('summaryarea'),
             style => 'margin: 0 10px 0 10px;'},
            div({align => 'center'},h3($display_line)),br,
            div({style => 'float: left;margin-right: 40px;'},
                table({class => 'summary'},
                      Tr(th({colspan => 2},'Line data')),
                      map {Tr(td([a({href=>'#','data-toggle'=>'tooltip',title=>$lp->{$_}{definition}},$_),
                                  $lp->{$_}{value}]))} sort keys %$lp)),
            $pd,$lr,$gt,
            div({style=>'clear:both;'},NBSP),
           );

  my($arrow,$display) = ('down','display:block;');
  &renderLightImagery($line,$arrow,$display);
  &renderFlyOlympiad($line,$arrow,$display);
  ($arrow,$display) = ('right','display:none;');
  &renderLarvalOlympiad($line,$arrow,$display);
  &renderCrossesFlips($line,$arrow,$display);
  &renderFlyStore($line,$arrow,$display);
  &renderWIP($line,$arrow,$display);

  # ----- Footer -----
  print end_form,&sessionFooter($Session),end_html;
}


sub getLineRel
{
  $sth{LINEREL}->execute(my $line = shift);
  my $ar = $sth{LINEREL}->fetchall_arrayref;
  my $lr = NBSP;
  if (scalar @$ar) {
    my (@tp,@tc);
    foreach (@$ar) {
      my $link = a({href => '?line='.$_->[1]},$_->[1]);
      $link .= " ($_->[2])" if ($_->[2]);
      if ($_->[0] eq 'child_of') {
        push @tc,$link;
      }
      else {
        push @tp,$link;
      }
    }
    my @tr;
    push @tr,Tr(td({style => 'background-color: #3cc;'},'Parents'),
                td(((scalar @tc) ? join(br,@tc)
                    : span({style => 'font-style: italic;'},'None'))));
    push @tr,Tr(td({style => 'background-color: #3cc;'},'Children'),
                td(((scalar @tp) ? join(br,@tp)
                    : span({style => 'font-style: italic;'},'None'))));
    $lr = div({style => 'float: left;margin-right: 40px;'},
              table({class => 'relationships'},
                    Tr(th({colspan => 2},'Line relationships')),@tr));
  }
  return($lr);
}


sub getGenomic
{
  my $lp = shift;
  my %genomic;
  delete(@{$lp}{'Stain'});
  foreach ('# residues','Chromosome','Left primer','Right primer',
           'Minimum coordinate','Maximum coordinate','Strand') {
    if (exists $lp->{$_}) {
      $genomic{$_} = $lp->{$_}{value};
      delete($lp->{$_});
    }
  }
  if ($genomic{'Chromosome'} && $genomic{'Minimum coordinate'}
      && $genomic{'Maximum coordinate'}) {
    $genomic{Coordinates} = sprintf '%s: %d-%d',
      delete @genomic{('Chromosome','Minimum coordinate','Maximum coordinate')};
  }
  my $gt = (scalar keys %genomic)
           ? table({class => 'summary'},
                   Tr(th({colspan => 2},'Genomic data')),
                   map {Tr(td([$_,$genomic{$_}]))} sort keys %genomic)
           : NBSP;
  return($gt);
}


sub getPublishing
{
  my $pd = NBSP;
  $sth{PUBLISHING}->execute(my $line = shift);
  my $ar = $sth{PUBLISHING}->fetchall_arrayref;
  if (scalar @$ar) {
    $pd = span({style => 'font-weight: bold'},'Publishing names')
          . table({class => 'standard'},
                  Tr(th(['Name','Requester','Published','Label'])),
                 map {Tr(td($_))} @$ar);
    $sth{PUBLISHED}->execute($line);
    $ar = $sth{PUBLISHED}->fetchall_arrayref;
    if (scalar @$ar) {
      $pd .= span({style => 'font-weight: bold'},'Publishing data')
             . table({class => 'standard'},
                     Tr(th(['Name','Release','Images'])),
                    map {Tr(td($_))} @$ar);
    }
  }
  return($pd);
}


sub renderFlyOlympiad
{
  my($line,$arrow,$display) = @_;
  my $content = '';
  $sth{ARENA}->execute($line);
  my $ar = $sth{ARENA}->fetchall_arrayref();
  $sth{EXP}->execute($line);
  my $ar2 = $sth{EXP}->fetchall_arrayref();
  @$ar = (@$ar,@$ar2);
  if (scalar @$ar) {
    foreach (@$ar) {
      my $t = $_->[0];
      $_->[0] = a({href   => $LINK_MAP{$t} . (($t =~ /lethality/) ? '' : $line),
                   title  => 'Open experiment data in a new window',
                target => '_blank'},$t) if (exists $LINK_MAP{$t});
    }
    $content .= table({class => 'tablesorter',&identify('table1')},
                      thead(Tr(th(['Assay','Experiment','Experiment date']))),
                      tbody(map { Tr(td([@$_])) } @$ar));
    &printSection('exp','Fly Olympiad experiments',$arrow,$display,\$content,
                  scalar(@$ar),'flyolympiad.png');
  }
}


sub renderLarvalOlympiad
{
  my($line,$arrow,$display) = @_;
  my $content = '';
  $sthl{EXP}->execute($line);
  my $ar = $sthl{EXP}->fetchall_arrayref();
  if (scalar @$ar) {
    my @arr = @$ar;
    @$ar = ();
    foreach (@arr) {
      my($tracker,$line_eff,$sa,$date) = split('/',shift @$_);
      next unless ($date);
      my($stimpro,$animalno) = split('@',$sa);
      unshift @$_,$tracker,$line_eff,$stimpro,$animalno,$date;
      push @$ar,$_;
    }
    $content .= table({class => 'tablesorter',&identify('table2')},
                      thead(Tr(th(['Tracker','Line/Effector',
                                   'Stimulus/Protocol','Animal','Timestamp',
                                   '# sessions']))),
                      tbody(map { Tr(td([@$_])) } @$ar));
    &printSection('larvalexp','Larval Olympiad experiments',$arrow,$display,
                  \$content,scalar(@$ar),'larvalolympiad.png');
  }
}


sub renderLightImagery
{
  my($line,$arrow,$display) = @_;
  my $content = '';
  $sth{IMAGES}->execute($line);
  my $ar = $sth{IMAGES}->fetchall_arrayref();
  if (scalar @$ar) {
    foreach (@$ar) {
      if ($_->[1] =~ /(?:jpg|png)$/) {
        $sth{IMAGEU}->execute($_->[1]);
        my($url) = $sth{IMAGEU}->fetchrow_array();
        $_->[2] = a({href => $url,
                     target => '_blank'},
                    img({src => $url . '?proportional=yes&height=60'}));
      }
      elsif ($_->[2]) {
        my $suffix = '?proportional=yes&height=60';
        my %options = ();
        if ($_->[2] =~ /(?:WebDAV|jade)/i) {
          $suffix = '';
          %options = (height => 60);
        }
        $_->[2] = a({href => $_->[2],
                     target => '_blank'},
                    img({src => $_->[2] . $suffix,
                         %options}));
      }
      $_->[2] = div({class => 'projection',
                     style => 'display: none;'},$_->[2]) if ($_->[2]);
      my($lsmname) = (split('/',$_->[1]))[-1];
      $_->[1] = a({href => 'http://webstation.int.janelia.org/search?term=' . $lsmname,
                   target => '_blank'},$_->[1]);
    }
    $content .= a({href => "?line=$line;mode=ibl",
                   target => '_blank'},'Show all image products') . br;
    $content .= checkbox(&identify('projections'),
                         -label => 'Show projections',
                         -checked => 0,
                         -onClick => 'toggleProjections();') .
                table({class => 'tablesorter',&identify('table3')},
                      thead(Tr(th(['Image family','Image name','Projection']))),
                      tbody(map { Tr(td([@$_])) } @$ar));
    &printSection('imagery','Imagery',$arrow,$display,\$content,scalar(@$ar),
                  'flybrain.png');
  }
}


sub renderCrossesFlips
{
  my($line,$arrow,$display) = @_;
  my($ccontent,$fcontent) = ('')x2;
  $sthf{SH}->execute($line,$line);
  my $ar = $sthf{SH}->fetchall_arrayref();
  my (@arc,@arf);
  if (scalar @$ar) {
    foreach (@$ar) {
      if ($_->[1] eq 'cross') {
        splice @$_,1,1;
        my $cn = ($_->[1] eq $line) ? 1 : 2;
        $_->[$cn] = span({style => 'font-weight: bold'},$_->[$cn]);
        push @arc,$_;
      }
      else {
        splice @$_,2,8;
        push @arf,$_;
      }
    }
    $ccontent .= table({class => 'tablesorter',&identify('table4')},
                       thead(Tr(th(['Date','Line 1','Line 2','Effector','Project','Project lab','Cross type','Cross barcode','Wish list']))),
                       tbody(map { Tr(td([@$_])) } @arc));
    $fcontent .= table({class => 'tablesorter',&identify('table4')},
                       thead(Tr(th(['Date','Event']))),
                       tbody(map { Tr(td([@$_])) } @arf));
    &printSection('crosses','Crosses',$arrow,$display,\$ccontent,
                  scalar(@arc),'flycross.png')
      if (scalar(@arc) && ($CLASS !~ /3/) || $VIEW);
    &printSection('flips','Flips/Copies',$arrow,$display,\$fcontent,
                  scalar(@arf),'flyrobot.png') if (scalar @arf);
  }
}


sub renderFlyStore
{
  my($line,$arrow,$display) = @_;
  my $content = '';
  $sthf{FLYSTORE}->execute($line);
  my $ar = $sthf{FLYSTORE}->fetchall_arrayref();
  if (scalar @$ar) {
    $content .= table({class => 'tablesorter',&identify('table6')},
                      thead(Tr(th(['Date ordered','Ordered for','Status','Order filled']))),
                      tbody(map { Tr(td([@$_])) } @$ar));
    &printSection('flystore','FlyStore',$arrow,$display,\$content,
                  scalar(@$ar),'flystore.png');
  }
}


sub renderWIP
{
  my($line,$arrow,$display) = @_;
  my $content = '';
  $sthw{ACTIVE}->execute($line);
  my $ar = $sthw{ACTIVE}->fetchall_arrayref();
  if (scalar @$ar) {
    foreach (@$ar) {
      my($process,$action,$timestamp) = @$_;
      $content .= sprintf '%s %s %s at %s',
                          a({href => $LINK_MAP{WIPL} . $line,
                             target => '_blank'},$line),
                          ($action eq 'in') ? 'entered' : 'exited',
                          $process,$timestamp;
      last;
    }
  }
  $sthw{BATCHESL}->execute($line);
  $ar = $sthw{BATCHESL}->fetchall_arrayref();
  if (scalar @$ar) {
    my @batch;
    foreach (@$ar) {
      push @batch,
           a({href => $LINK_MAP{WIPB} . $_->[0],
              target => '_blank'},$_->[0]),
    }
    $content .= br . 'Present in batches: ' . join(', ',@batch);
    &printSection('wip','WIP',$arrow,$display,\$content,'','wip.png');
  }
}


sub printSection {
  my($id,$title,$arrow,$display,$content_ref,$count,$image) = @_;
  $count = span({style => 'font-weight: bold'},$count) if ($count);
  $title .= " ($count)" if (length($count));
  ($arrow,$display) = ('right','display:none;') unless ($count);
  print div({style=>'clear:both;'},NBSP),
        div({class => 'boxed',
             style => 'margin: 0 10px 0 10px;'},
            table({width => '100%'},
                  Tr(td({width => '20%'},
                        a({href => '#',
                           onClick => 'toggleVis("'.$id.'"); return false;'},
                          img({&identify('i'.$id),
                               style => 'vertical-align:middle;',
                               src => '/images/' . $arrow . '_triangle.png'})
                        )
                        . (($image) ? img({style => 'vertical-align:middle;',
                                           src => "/images/$image"}) : '')
                       ),
                     td({width => '60%',
                         align => 'center'},h3($title)),
                     td({width => '20%'},NBSP))),br,
            div({&identify($id),style=>$display},$$content_ref),
           );
}


# ****************************************************************************
# * Subroutine:  hiddenParameters                                            *
# * Description: This routine will return HTML for hidden parameters.        *
# *                                                                          *
# * Parameters:  NONE                                                        *
# * Returns:     HTML                                                        *
# ****************************************************************************
sub hiddenParameters
{
  hidden(&identify('_database'),default => $DATABASE)
  . hidden(&identify('_operator'),default=>$OPERATOR);
}


# ****************************************************************************
# * Subroutine:  pageHead                                                    *
# * Description: This routine will return the page header.                   *
# *                                                                          *
# * Parameters:  Named parameters                                            *
# *              title: page title                                           *
# * Returns:     HTML                                                        *
# ****************************************************************************
sub pageHead
{
  my %arg = (title => $APPLICATION,
             mode => '',
             @_);
  my @scripts = ();
  my @styles = ();
  my %load = ();
  my (@script_file,@style_file);
  if ($arg{mode} eq 'summary') {
    push @style_file,qw();
    $load{load} = "initSummary();";
  }
  elsif ($arg{mode} eq 'detail') {
    push @script_file,qw(jquery/jquery_tabs jquery/jquery.tablesorter tablesorter);
    push @style_file,qw(jquery/redmond/theme tablesorter-blue);
    $load{load} = "tableInitialize(); tooltipInitialize();";
  }
  else {
    push @script_file,qw(jquery/jquery_tabs);
    push @style_file,qw(jquery/redmond/theme);
    $load{load} .= 'load_lines();' if (param('line_file'));
  }
  unshift @script_file,qw(jquery/jquery-ui-latest sorttable);
  push @scripts,map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    (@script_file,$PROGRAM);
  push @styles,map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   (@style_file);
  &standardHeader(title      => $arg{title},
                  css_prefix => $PROGRAM,
                  script     => \@scripts,
                  style      => \@styles,
                  breadcrumbs => \@BREADCRUMBS,
                  expires    => 'now',
                  %load);
}
