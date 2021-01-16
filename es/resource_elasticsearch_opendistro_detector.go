package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/structure"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/olivere/elastic/uritemplates"

	elastic7 "github.com/olivere/elastic/v7"
	elastic6 "gopkg.in/olivere/elastic.v6"
)

var openDistroDetectorSchema = map[string]*schema.Schema{
	"body": {
		Type:             schema.TypeString,
		Required:         true,
		DiffSuppressFunc: diffSuppressDetector,
		StateFunc: func(v interface{}) string {
			json, _ := structure.NormalizeJsonString(v)
			return json
		},
		ValidateFunc: validation.StringIsJSON,
	},
}

func resourceElasticsearchOpenDistroDetector() *schema.Resource {
	return &schema.Resource{
		Create: resourceElasticsearchOpenDistroDetectorCreate,
		Read:   resourceElasticsearchOpenDistroDetectorRead,
		Update: resourceElasticsearchOpenDistroDetectorUpdate,
		Delete: resourceElasticsearchOpenDistroDetectorDelete,
		Schema: openDistroDetectorSchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

func resourceElasticsearchOpenDistroDetectorCreate(d *schema.ResourceData, m interface{}) error {
	res, err := resourceElasticsearchOpenDistroPostDetector(d, m)

	if err != nil {
		log.Printf("[INFO] Failed to put Detector: %+v", err)
		return err
	}

	d.SetId(res.ID)
	log.Printf("[INFO] Object ID: %s", d.Id())

	// Although we receive the full Detector in the response to the POST,
	// OpenDistro seems to add default values to the ojbect after the resource
	// is saved, e.g. adjust_pure_negative, boost values
	return resourceElasticsearchOpenDistroDetectorRead(d, m)
}

func resourceElasticsearchOpenDistroDetectorRead(d *schema.ResourceData, m interface{}) error {
	res, err := resourceElasticsearchOpenDistroGetDetector(d.Id(), m)

	if elastic6.IsNotFound(err) || elastic7.IsNotFound(err) {
		log.Printf("[WARN] Detector (%s) not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}

	if err != nil {
		return err
	}

	d.SetId(res.ID)

	DetectorJson, err := json.Marshal(res.Detector)
	if err != nil {
		return err
	}
	DetectorJsonNormalized, err := structure.NormalizeJsonString(string(DetectorJson))
	if err != nil {
		return err
	}
	err = d.Set("body", DetectorJsonNormalized)
	return err
}

func resourceElasticsearchOpenDistroDetectorUpdate(d *schema.ResourceData, m interface{}) error {
	_, err := resourceElasticsearchOpenDistroPutDetector(d, m)

	if err != nil {
		return err
	}

	return resourceElasticsearchOpenDistroDetectorRead(d, m)
}

func resourceElasticsearchOpenDistroDetectorDelete(d *schema.ResourceData, m interface{}) error {
	var err error

	path, err := uritemplates.Expand("/_opendistro/_anomaly_detection/detectors/{id}", map[string]string{
		"id": d.Id(),
	})
	if err != nil {
		return fmt.Errorf("error building URL path for Detector: %+v", err)
	}

	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		_, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "DELETE",
			Path:   path,
		})
	case *elastic6.Client:
		_, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "DELETE",
			Path:   path,
		})
	default:
		err = errors.New("Detector resource not implemented prior to Elastic v6")
	}

	return err
}

func resourceElasticsearchOpenDistroGetDetector(DetectorID string, m interface{}) (*DetectorResponse, error) {
	var err error
	response := new(DetectorResponse)

	path, err := uritemplates.Expand("/_opendistro/_anomaly_detection/detectors/{id}", map[string]string{
		"id": DetectorID,
	})
	if err != nil {
		return response, fmt.Errorf("error building URL path for Detector: %+v", err)
	}

	var body json.RawMessage
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return nil, err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		var res *elastic7.Response
		res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "GET",
			Path:   path,
		})
		body = res.Body
	case *elastic6.Client:
		var res *elastic6.Response
		res, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "GET",
			Path:   path,
		})
		body = res.Body
	default:
		err = errors.New("Detector resource not implemented prior to Elastic v6")
	}

	if err != nil {
		return response, err
	}

	if err := json.Unmarshal(body, response); err != nil {
		return response, fmt.Errorf("error unmarshalling Detector body: %+v: %+v", err, body)
	}
	normalizeDetector(response.Detector)
	return response, err
}

func resourceElasticsearchOpenDistroPostDetector(d *schema.ResourceData, m interface{}) (*DetectorResponse, error) {
	DetectorJSON := d.Get("body").(string)

	var err error
	response := new(DetectorResponse)

	path := "/_opendistro/_anomaly_detection/detectors/"

	var body json.RawMessage
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return nil, err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		var res *elastic7.Response
		res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "POST",
			Path:   path,
			Body:   DetectorJSON,
		})
		body = res.Body
	case *elastic6.Client:
		var res *elastic6.Response
		res, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "POST",
			Path:   path,
			Body:   DetectorJSON,
		})
		body = res.Body
	default:
		err = errors.New("Detector resource not implemented prior to Elastic v6")
	}

	if err != nil {
		return response, err
	}

	if err := json.Unmarshal(body, response); err != nil {
		return response, fmt.Errorf("error unmarshalling Detector body: %+v: %+v", err, body)
	}
	normalizeDetector(response.Detector)
	return response, nil
}

func resourceElasticsearchOpenDistroPutDetector(d *schema.ResourceData, m interface{}) (*DetectorResponse, error) {
	DetectorJSON := d.Get("body").(string)

	var err error
	response := new(DetectorResponse)

	path, err := uritemplates.Expand("/_opendistro/_anomaly_detection/detectors/{id}", map[string]string{
		"id": d.Id(),
	})
	if err != nil {
		return response, fmt.Errorf("error building URL path for Detector: %+v", err)
	}

	var body json.RawMessage
	esClient, err := getClient(m.(*ProviderConf))
	if err != nil {
		return nil, err
	}
	switch client := esClient.(type) {
	case *elastic7.Client:
		var res *elastic7.Response
		res, err = client.PerformRequest(context.TODO(), elastic7.PerformRequestOptions{
			Method: "PUT",
			Path:   path,
			Body:   DetectorJSON,
		})
		body = res.Body
	case *elastic6.Client:
		var res *elastic6.Response
		res, err = client.PerformRequest(context.TODO(), elastic6.PerformRequestOptions{
			Method: "PUT",
			Path:   path,
			Body:   DetectorJSON,
		})
		body = res.Body
	default:
		err = errors.New("Detector resource not implemented prior to Elastic v6")
	}

	if err != nil {
		return response, err
	}

	if err := json.Unmarshal(body, response); err != nil {
		return response, fmt.Errorf("error unmarshalling Detector body: %+v: %+v", err, body)
	}

	return response, nil
}

type DetectorResponse struct {
	Version int                    `json:"_version"`
	ID      string                 `json:"_id"`
	Detector map[string]interface{} `json:"Detector"`
}
