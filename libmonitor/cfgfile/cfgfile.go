package cfgfile

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func Read(out interface{}, path string) error {
	if path == "" {
		path = "默认路径"
	}

	filecontent, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Failed to read %s: %v. Exiting.\n", path, err)
	}

	if err = yaml.Unmarshal(filecontent, out); err != nil {
		return fmt.Errorf("YAML config parsing failed on")
	}
	return nil
}
