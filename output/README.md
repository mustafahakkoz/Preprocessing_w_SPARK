## Output folder

A json file (col_17M_calculations_rf.json in our example) will be produced in a format defined in mainpage of repository. For an example:

```json
{  
	"id":"1001846898876133376",  
	"user_features_rf":{  
		"dict_activeness_1":{"ekonomi":0.4084507,"kultursanat":0.4583333,"politika":0.8732394,"spor":0.3802817,"teknoloji":0.3333333},  
		"dict_activeness_2":{"ekonomi":0.0178634,"kultursanat":0.0250343,"politika":0.1750344,"spor":0.0138494,"teknoloji":0.0117851},  
		"dict_activeness_3":{"ekonomi":0.0112987,"kultursanat":0.0123303,"politika":0.1550305,"spor":0.0060264,"teknoloji":0.0044323},  
		"dict_focus_rate":{"ekonomi":0.072238,"kultursanat":0.1033994,"politika":0.7209632,"spor":0.0552408,"teknoloji":0.0481586}},  
	"centralities_rf":{  
		"betweennessCentrality":{"ekonomi":0.5,"kultursanat":0.0,"politika":0.0,"spor":0.0,"teknoloji":0.0},  
		"closenessCentrality":{"ekonomi":0.0,"kultursanat":0.0,"politika":0.1336288,"spor":0.0,"teknoloji":0.0},  
		"degreeCentrality":{"ekonomi":0.0,"kultursanat":0.0,"politika":0.0017036,"spor":0.0,"teknoloji":0.0},  
		"pageRank":{"ekonomi":0.1468788,"kultursanat":0.0100135,"politika":0.0029293,"spor":0.0198249,"teknoloji":0.0028019}}  
}
```

File name can be changed in Main.py.