package client

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadSchemaDir(t *testing.T) {
	inputSchemas := map[string][]byte{
		"a.json": []byte(`{
			"name": "a"
		}`),
		"b.json": []byte(`{
			"name": "b"
		}`),
		"c.json": []byte(`{
			"name": "c"
		}`),
		"d": []byte(`{
			"name": "d"
		}`),
	}

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	for fileName, schema := range inputSchemas {
		require.NoError(t, ioutil.WriteFile(path.Join(tmpDir, fileName), schema, 0777))
	}

	schemas, err := readSchemaDir(tmpDir)
	require.NoError(t, err)

	// a/b/c should be present, but not d becuase its not a .json file.
	require.Equal(t, map[string]schema{
		"a.json": {
			name:  "a",
			bytes: inputSchemas["a.json"],
		},
		"b.json": {
			name:  "b",
			bytes: inputSchemas["b.json"],
		},
		"c.json": {
			name:  "c",
			bytes: inputSchemas["c.json"],
		},
	}, schemas)

	// Add an invalid file now and make sure it errors out.
	require.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "e.json"), []byte("invalid"), 0777))
	_, err = readSchemaDir(tmpDir)
	require.Error(t, err)
}
