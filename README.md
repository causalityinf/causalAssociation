# README for the Causal Associations between Temporal Events

## Paper abstract
Causal inference from observational data has been widely studied to infer causal relations between causes and effects. Due to the popularity of event-based data, causal inference from event datasets has attracted increasing interest. However, inferring causal associations from observational event sequences is challenging because of the heterogeneous and irregular nature of event-based data. Existing work on causal inference for temporal events disregards the event durations, and is thus unable to capture their impact on the causal associations. In the present paper, we overcome this limitation by proposing a new modeling approach for temporal events that captures and utilizes event durations. Based on this new temporal model, we propose a set of novel Duration-based Event Causality (DEC) scores, including the Duration-based Necessity and Sufficiency Trade-off score, and the Duration-based Conditional Intensity Rates scores that take into consideration event durations when capturing causal associations between temporal events. We prove that the proposed scores follow the causality hypothesis testing framework. We conduct an extensive experimental evaluation using both synthetic datasets, and two real-world event datasets in the medical and environmental domains to evaluate our proposed scores, and compare them against the closest baseline. The experimental results show that our proposed scores outperforms the baseline with a large margin using the popular evaluation metric Hits@K.

## Prerequisites
- Python 3.8 (or later)
- pandas 1.0.3 (or later)

## To compute DNST
```
python3 source/DEC.py --nst -I path_input -O path_output --cause name_col_causality --effect name_col_effect --duration name_col_duration
```
  
path_input: path of input dataset  
path_output: path of output folder  
name_col_causality: name of cause column  
name_col_effect: name of effect column  
name_col_duration: name of duration column  

Example:  
```
python3 source/DEC.py --nst -I data/air/preprocessedData/Air_PM10_Duration.csv -O result --cause cause --effect effect --duration duration
```

## To compute DCIR<sub>P</sub>
```
python3 source/DEC.py --cirb -I path_input -O path_output --cause name_col_causality --effect name_col_effect --duration name_col_duration
```
  
path_input: path of input dataset  
path_output: path of output folder  
name_col_causality: name of cause column  
name_col_effect: name of effect column   
name_col_duration: name of duration column  

Example:  
```
python3 source/DEC.py --cirb -I data/air/preprocessedData/Air_PM10_Duration.csv -O result --cause cause --effect effect --duration duration
```

## To compute DCIR<sub>N</sub>
```
python3 source/DEC.py --circ -I path_input -O path_output --cause name_col_causality --effect name_col_effect --duration name_col_duration
```
  
path_input: path of input dataset  
path_output: path of output folder  
name_col_causality: name of cause column  
name_col_effect: name of effect column   
name_col_duration: name of duration column  

Example:  
```
python3 source/DEC.py --circ -I data/air/preprocessedData/Air_PM10_Duration.csv -O result --cause cause --effect effect --duration duration
```

## To compute DCIR<sub>M</sub>
```
python3 source/DEC.py --cirm -I path_input -O path_output --cause name_col_causality --effect name_col_effect --duration name_col_duration --parent path_parent
```
  
path_input: path of input dataset  
path_output: path of output folder  
name_col_causality: name of cause column  
name_col_effect: name of effect column   
name_col_duration: name of duration column  
path_parent: path of parent file 

Example:  
```
python3 source/DEC.py --cirm -I data/air/preprocessedData/Air_PM10_Duration.csv -O result --cause cause --effect effect --duration duration --parent parent/air/parent_PM10.json
```
