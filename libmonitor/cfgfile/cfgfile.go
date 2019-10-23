package cfgfile

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func Read(out interface{}, path string) error {
	if path == "" {
		path = "/Users/a1800101257/go/src/github.com/ssp4599815/monitors/redis/config/config.yaml"
	}

	filecontent, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Failed to read %s: %v. Exiting.\n", path, err)
	}

	if err = yaml.Unmarshal(filecontent, out); err != nil {
		fmt.Println(err)
		return fmt.Errorf("YAML config parsing failed on")
	}
	return nil
}
