<?php
session_start();
$self_url = $_SERVER['REQUEST_URI'];
$_SESSION['referer'] = $self_url;
require_once('functions_common.php');
if (!isset($_SESSION['name'])) {
  header("Location: ldap.php");
  exit;
}
$js = array("highcharts/highcharts.js");
$header = generate_header($js,array(),'',"Process flow");
$footer = generate_footer();
echo $header;
chooseFlow();
$stepnum = 1;
$substep = array();
if ((isset($_REQUEST['flow']))
     && ($_REQUEST['flow'] != 'Select a process flow...')) {
  parseInput($_REQUEST['flow']);
}
elseif ((isset($_REQUEST['dataset']))
     && ($_REQUEST['dataset'] != 'Select a dataset...')) {
  parseInput($_REQUEST['dataset']);
}
echo "<div style='clear:both;'></div>",$footer;


function chooseFlow() {
  $flow = array();
  foreach (glob("/usr/local/pipeline/process/*.process") as $file) {
    array_push($flow,preg_replace('/\.process/','',preg_replace('/.*\//','',$file)));
  }
  echo '<form><select name="flow" onchange="this.form.submit()">';
  echo "<option selected='true'>Select a process flow...</option>";
  foreach ($flow as $f) {
    $opt = '';
    echo "<option $opt>$f</option>";
  }
  echo '</select> OR ';

  if (0) {
  $db = mysql_pconnect('prd-db','flyportalRead','flyportalRead')
    or die('Could not connect to Janelia Workstation database: '.mysql_error());
  $selected = mysql_select_db('flyportal')
    or die('Unable to select flyportal database');
  $sql = "SELECT dsi.value,CONCAT('PipelineConfig_',pp.value) FROM entity ds JOIN entityData dsi ON "
         . "(ds.id=dsi.parent_entity_id AND dsi.entity_att='Data Set Identifier') "
         . "JOIN entityData pp ON (pp.parent_entity_id=dsi.parent_entity_id "
         . "AND pp.entity_att='Pipeline Process') WHERE "
         . "ds.entity_type='Data Set' AND pp.value != '' ORDER BY 1";
  $result = mysql_query($sql,$db) or die('Query failed: ' . $sql);
  echo '<select name="dataset" onchange="this.form.submit()">';
  echo "<option selected='true'>Select a dataset...</option>";
  while ($row = mysql_fetch_array($result)) {
    if (in_array($row[1],$flow) || strpos($row[1],','))
      echo "<option value='$row[1]'>$row[0]</option>";
  }
  echo '</select>';
  }

  $json = file_get_contents("/opt/informatics/data/workstation_ng.json");
  $response = json_decode($json,true);
  $json = file_get_contents($response['url'] . $response['query']['DatasetPipeline']);
  $response = json_decode($json,true);
  ksort($response);
  echo '<select name="dataset" onchange="this.form.submit()">';
  echo "<option selected='true'>Select a dataset...</option>";
  foreach ($response as $k => $v) {
    echo "<option value='$v'>$k</option>";
  }
  echo '</select>';
  echo '</form>';
}


function parseInput($flow) {
  if (strpos($flow,',')) {
    $flowlist = array();
    foreach (explode(',',$flow) as $p) {
      if (!preg_match('/^PipelineConfig_/',$p))
        $p = 'PipelineConfig_' . $p;
      array_push($flowlist,$p);
    }
    printf("This process flow consists of %d serially executed process flows:<br>",
           count($flowlist));
    echo "<div style='margin-left: 20px'>";
    foreach ($flowlist as $f)
      echo "<a href='#$f'>$f</a><br>";
    echo "</div>";
    foreach ($flowlist as $f) {
      echo "<a name='$f'></a>";
      showFlow($f);
    }
  }
  else {
    showFlow($flow);
  }
}


function showFlow($flow) {
$xml = simplexml_load_file("/usr/local/pipeline/process/$flow.process");
echo "<h2>",$xml->attributes()->name,"</h2>";
$attr = $xml->attributes();
if ($attr) {
  foreach ($attr as $a => $b) {
    if ($a != 'name')
      echo "$a: $b<br>";
  }
  echo "<br>";
}
echo "<div class='boxed'>Legend:<br>";
$color = 0;
$colors = array('cff','cfc','fcc','ccf','cc6','fc9','f66');
foreach (array('Operation','Include','If-then','For loop','Wait for async','Sequence','Exception') as $c) {
  echo "<div style='background-color: #" . $colors[$color++] . "; width: 110px; margin-right: 10px; float: left; text-align: center;'>"
       . $c . "</div>";
}
echo "<br></div><br><br>";
foreach($xml->children() as $child) {
  processNode($child);
}
echo '<div style="float:left;">';
echo '</div>';
}


function processNode($node) {
  $step = $node->getName();
  global $substep;
  if (in_array($step,array('exceptionHandler','include',
                           'operation','sequence'))
      && count($substep)) {
    printArrowDiv($substep,1);
    $substep = array();
  }
  if ($step == 'exceptionHandler') {
    processException($node);
  }
  elseif ($step == 'include') {
    processInclude($node);
  }
  elseif ($step == 'operation') {
    processOperation($node);
  }
  elseif ($step == 'sequence') {
    processSequence($node);
  }
  else {
    array_push($substep,$step . ":: " . $node->attributes()->name . "<br>");
  }
}


function processException($node) {
  echo "<div class='exception'>";
  echo "EXCEPTION";
  echo "<div style='margin-left: 20px;'>";
  foreach($node->children() as $child) {
    processNode($child);
  }
  echo "</div><br></div>";
}


function processInclude($node) {
  echo "<div class='include'>";
  echo $node->attributes()->name . " ("
       . "<a href='?flow=" . $node->attributes()->process . "' target='_blank'>"
       . $node->attributes()->process . "</a>)<br>";
  processLeaves($node);
  echo "</div>";
}


function processOperation($node) {
  echo "<div class='operation'>";
  echo $node->attributes()->name . " ("
       . $node->attributes()->processor . ")<br>";
  processLeaves($node);
  echo "</div>";
}


function processSequence($node) {
  $attr = $node->attributes();
  $attribute = array();
  if ($attr) {
    foreach ($attr as $a => $b) {
      $attribute[$a] = $b;
    }
  }
  if (array_key_exists('if',$attribute)) {
    echo "<div class='sequenceif'>";
  }
  elseif (array_key_exists('forEach',$attribute)) {
    echo "<div class='sequenceforeach'>";
  }
  elseif (array_key_exists('waitForAsync',$attribute)
          && ($attribute['waitForAsync'] == 'true')) {
    echo "<div class='waitforasync'>Wait for async";
    if (array_key_exists('name',$attribute)) {
      echo " (" . $attribute['name'] . ")";
      unset($attribute['name']);
    }
    unset($attribute['waitForAsync']);
    echo "<br>";
  }
  else {
    echo "<div class='sequence'>SEQUENCE";
    if (array_key_exists('name',$attribute)) {
      echo " (" . $attribute['name'] . ")";
      unset($attribute['name']);
    }
    echo "<br>";
  }

  foreach ($attribute as $a => $b) {
    echo "$a $b<br>";
  }
  echo "<div style='margin-left: 20px;'>";
  foreach($node->children() as $child) {
    processNode($child);
  }
  echo "</div></div>";
}

function processLeaves($node) {
  $leaves = array();
  foreach($node->children() as $child) {
    $txt = $child->getName() . ": " . $child->attributes()->name;
    if ($child->attributes()->value)
      $txt .= "=" . $child->attributes()->value;
    $txt .= "<br>";
    array_push($leaves,$txt);
  }
  printArrowDiv($leaves,0);
}

function printArrowDiv($leaves,$noindent) {
  if (count($leaves)) {
    global $stepnum;
    if (!$noindent)
      echo "<div style='margin-left: 20px;'>";
    echo "<a onclick='toggleVis(" . '"s' . $stepnum
         . '"' . ");'>" . "<img id='is$stepnum' "
         . "src='/images/right_triangle_small.png'></a>";
    echo "Input/Output";
    echo "<div class='iolist' id='s$stepnum'>";
    foreach ($leaves as $l) {
      echo $l;
    }
    if (!$noindent)
      echo "</div>";
    echo "</div>";
    $stepnum++;
  }
}


?>
