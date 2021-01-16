package es

import (
	"fmt"
	"testing"

	elastic7 "github.com/olivere/elastic/v7"
	elastic5 "gopkg.in/olivere/elastic.v5"
	elastic6 "gopkg.in/olivere/elastic.v6"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccElasticsearchOpenDistroDetector(t *testing.T) {
	provider := Provider().(*schema.Provider)
	err := provider.Configure(&terraform.ResourceConfig{})
	if err != nil {
		t.Skipf("err: %s", err)
	}
	meta := provider.Meta()
	esClient, err := getClient(meta.(*ProviderConf))
	if err != nil {
		t.Skipf("err: %s", err)
	}
	var allowed bool

	switch esClient.(type) {
	case *elastic5.Client:
		allowed = false
	default:
		allowed = true
	}

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			if !allowed {
				t.Skip("Detectors only supported on >= ES 6")
			}
		},
		Providers:    testAccOpendistroProviders,
		CheckDestroy: testCheckElasticsearchDetectorDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccElasticsearchOpenDistroDetector,
				Check: resource.ComposeTestCheckFunc(
					testCheckElasticsearchOpenDistroDetectorExists("elasticsearch_opendistro_detector.test_detector"),
				),
			},
		},
	})
}

func testCheckElasticsearchOpenDistroDetectorExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No Detector ID is set")
		}

		meta := testAccOpendistroProvider.Meta()

		var err error
		esClient, err := getClient(meta.(*ProviderConf))
		if err != nil {
			return err
		}
		switch esClient.(type) {
		case *elastic7.Client:
			_, err = resourceElasticsearchOpenDistroGetDetector(rs.Primary.ID, meta.(*ProviderConf))
		case *elastic6.Client:
			_, err = resourceElasticsearchOpenDistroGetDetector(rs.Primary.ID, meta.(*ProviderConf))
		default:
		}

		if err != nil {
			return err
		}

		return nil
	}
}

func testCheckElasticsearchDetectorDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "elasticsearch_opendistro_Detector" {
			continue
		}

		meta := testAccOpendistroProvider.Meta()

		var err error
		esClient, err := getClient(meta.(*ProviderConf))
		if err != nil {
			return err
		}
		switch esClient.(type) {
		case *elastic7.Client:
			_, err = resourceElasticsearchOpenDistroGetDetector(rs.Primary.ID, meta.(*ProviderConf))

		case *elastic6.Client:
			_, err = resourceElasticsearchOpenDistroGetDetector(rs.Primary.ID, meta.(*ProviderConf))
		default:
		}

		if err != nil {
			return nil // should be not found error
		}

		return fmt.Errorf("Detector %q still exists", rs.Primary.ID)
	}

	return nil
}

var testAccElasticsearchOpenDistroDetector = `
resource elasticsearch_opendistro_detector detector {
  name = "detector"
  description = "something"
  body = <<EOT
{
	"name": "detector",
	"description": "something",
  "time_field":"@t",
  "indices":[
    "index-*"
  ],
  "filter_query":{
    "bool" : {
        "filter" : [
          {
            "prefix" : {
              "log_group" : {
                "value" : "group",
                "boost" : 1.0
              }
            }
          }
        ],
        "adjust_pure_negative" : true,
        "boost" : 1.0
      }
  },
  "detection_interval": {
    "period": {
      "interval": 5,
      "unit": "Minutes"
    },
    "window_delay": {
      "period": {
        "interval": 10,
        "unit": "Minutes"
      }
    }
  }
}
EOT
}
`
