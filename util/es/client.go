package es

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/olivere/elastic/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"log"
	"strings"
)

type RuleGroupDoc struct {
	Name     string        `json:"name"`
	Interval string        `json:"interval"`
	Rules    []RuleItemDoc `json:"rules"`
}

type RuleItemDoc struct {
	Name        string  `json:"name"`
	Query       string  `json:"query"`
	Duration    string  `json:"duration"`
	Labels      []Label `json:"labels"`
	Annotations []Label `json:"annotations"`
	Health      string  `json:"health"`
	LastError   string  `json:"lastError"`
	RuleType    string  `json:"ruleType"`
}

type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ESClient struct {
	Client *elastic.Client
	Index  string
	logger log.Logger
}

func NewESClient(url string) *ESClient {
	cfg, err := config.Parse(url)
	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		// Handle error
		panic(err)
	}
	es := &ESClient{Client: client, Index: "alert_rules"}
	return es
}

func (es *ESClient) ListRules() (rules []*RuleGroupDoc) {
	searchResult, err := es.Client.Search(es.Index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if searchResult.TotalHits() > 0 {
		log.Printf("Found a total of %d tweets\n", searchResult.TotalHits())
		for _, hit := range searchResult.Hits.Hits {
			var t RuleGroupDoc
			err := json.Unmarshal(*hit.Source, &t)
			if err != nil {
				log.Printf("unmarshal result error: %v\n", err)
				continue
			}
			rules = append(rules, &t)
		}
	} else {
		// No hits
		log.Print("Found no RuleGroup!\n")
	}
	return
}

func (item RuleItemDoc) IsAlert() bool {
	return strings.EqualFold("alert", item.RuleType)
}

func (item RuleItemDoc) GetLabels() labels.Labels {
	return item.fmtLabels(item.Labels)
}

func (item RuleItemDoc) GetAnnotations() labels.Labels {
	return item.fmtLabels(item.Annotations)
}

func (item RuleItemDoc) fmtLabels(items []Label) labels.Labels {
	if len(items) == 0 {
		return nil
	}
	lbs := make([]labels.Label, 0, len(items))
	for _, v := range items {
		lbs = append(lbs, labels.Label{Name: v.Name, Value: v.Value})
	}
	return lbs
}

func test() {
	//ctx := context.Background()
	cfg, err := config.Parse("http://172.24.28.1:9200/alert_rules?sniff=false")

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		// Handle error
		panic(err)
	}
	searchResult, err := client.Search("alert_rules").Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}

	if searchResult.TotalHits() > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())
		for _, hit := range searchResult.Hits.Hits {
			var t RuleGroupDoc
			err := json.Unmarshal(*hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}
			// Work with tweet
			fmt.Printf("RuleGroup by %s: %s\n", t.Name, t.Interval)
			//fmt.Printf("item name:%s \n", t.Rules[0].Name)
		}
	} else {
		// No hits
		fmt.Print("Found no tweets\n")
	}
}

func test1() {
	url := "http://172.24.28.1:9200/alert_rules?sniff=false"
	es := NewESClient(url)
	rules := es.ListRules()

	for _, v := range rules {
		log.Printf("rule %s, interval:%v\n", v.Name, v.Interval)
	}
}
