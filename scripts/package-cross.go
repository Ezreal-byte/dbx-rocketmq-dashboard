// Command package-cross builds the Go sidecar for one DBX target and writes an
// unsigned .dbxp review candidate plus its .artifact.json metadata.
//
// It exists because `dbx-plugin package` refuses to cross-compile native
// backends, while this plugin ships Windows 7 compatible artifacts that are
// built with the pinned go1.20.14 toolchain.
//
// Usage:
//
//	go run scripts/package-cross.go                       # current platform
//	go run scripts/package-cross.go -target windows-x64
//	go run scripts/package-cross.go -allow-non-win7       # skip the toolchain gate
package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// windows7GoPrefix is the last Go toolchain line that still supports Windows 7.
// DBX builds its own Windows 7 compatible agents (RabbitMQ, Oracle, ...) with
// go1.20.14 and asserts the same property at release time.
const windows7GoPrefix = "go1.20.14"

type artifactMetadata struct {
	Target string `json:"target"`
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type packageFile struct {
	name string
	data []byte
	mode fs.FileMode
}

func main() {
	defaultTarget := runtime.GOOS + "-" + mapRuntimeArch(runtime.GOARCH)
	target := flag.String("target", defaultTarget, "DBX target (windows-x64, windows-arm64, linux-x64, linux-arm64, darwin-x64, darwin-arm64)")
	outputDir := flag.String("output-dir", "dist", "output directory")
	goBinary := flag.String("go", defaultGoBinary(), "Go toolchain used to build the sidecar")
	allowNonWin7 := flag.Bool("allow-non-win7", false, "skip the go1.20.x toolchain check (produces a Windows 7 incompatible binary)")
	flag.Parse()

	parts := strings.Split(*target, "-")
	if len(parts) != 2 || !contains([]string{"windows", "linux", "darwin"}, parts[0]) || !contains([]string{"x64", "arm64"}, parts[1]) {
		fatalf("unsupported target %q", *target)
	}
	assertToolchain(*goBinary, *allowNonWin7)

	manifestBytes := mustRead("manifest.json")
	var manifest map[string]interface{}
	must(json.Unmarshal(manifestBytes, &manifest))
	id := manifest["id"].(string)
	version := manifest["version"].(string)
	entrypoints := manifest["entrypoints"].(map[string]interface{})
	backend := entrypoints["backend"].(map[string]interface{})
	binaryName := path.Base(backend["executable"].(string))
	if parts[0] == "windows" {
		binaryName += ".exe"
	}
	executable := path.Join("bin", *target, binaryName)
	backend["executable"] = executable
	manifestBytes, _ = json.MarshalIndent(manifest, "", "  ")
	manifestBytes = append(manifestBytes, '\n')

	temp, err := os.MkdirTemp("", "dbx-rocketmq-package-")
	must(err)
	defer os.RemoveAll(temp)
	binaryPath := filepath.Join(temp, binaryName)
	command := exec.Command(*goBinary, "build", "-trimpath", "-ldflags", "-s -w", "-o", binaryPath, ".")
	command.Dir = "backend"
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+parts[0], "GOARCH="+mapArch(parts[1]))
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	must(command.Run())
	if parts[0] == "windows" {
		audit := exec.Command("node", "scripts/assert-win7-pe-compat.mjs", binaryPath)
		audit.Stdout, audit.Stderr = os.Stdout, os.Stderr
		must(audit.Run())
	}

	files := []packageFile{{executable, mustRead(binaryPath), 0755}, {"manifest.json", manifestBytes, 0644}}
	for _, name := range []string{"LICENSE", "NOTICE", "PROVENANCE.md"} {
		files = append(files, packageFile{name, mustRead(name), 0644})
	}
	for _, root := range []string{"assets", "ui", "licenses"} {
		must(filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			// Studio is a single embedded document; do not ship stale CRA assets.
			if root == "ui" && filepath.ToSlash(filePath) != "ui/index.html" {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("unsupported package file %s", filePath)
			}
			files = append(files, packageFile{filepath.ToSlash(filePath), mustRead(filePath), 0644})
			return nil
		}))
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	checksums := make(map[string]string)
	for _, file := range files {
		digest := sha256.Sum256(file.data)
		checksums[file.name] = hex.EncodeToString(digest[:])
	}
	checksumBytes, _ := json.MarshalIndent(map[string]interface{}{"algorithm": "sha256", "files": checksums}, "", "  ")
	files = append(files, packageFile{"checksums.json", append(checksumBytes, '\n'), 0644})

	must(os.MkdirAll(*outputDir, 0755))
	archiveName := fmt.Sprintf("%s-%s-%s.dbxp", id, version, *target)
	archivePath := filepath.Join(*outputDir, archiveName)
	buffer := new(bytes.Buffer)
	writer := zip.NewWriter(buffer)
	fixed := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, file := range files {
		header := &zip.FileHeader{Name: file.name, Method: zip.Deflate, Modified: fixed}
		header.SetMode(file.mode)
		entry, err := writer.CreateHeader(header)
		must(err)
		_, err = entry.Write(file.data)
		must(err)
	}
	must(writer.Close())
	must(os.WriteFile(archivePath, buffer.Bytes(), 0644))
	digest := sha256.Sum256(buffer.Bytes())
	metadata := artifactMetadata{*target, archiveName, hex.EncodeToString(digest[:]), int64(buffer.Len())}
	metadataBytes, _ := json.MarshalIndent(metadata, "", "  ")
	must(os.WriteFile(strings.TrimSuffix(archivePath, ".dbxp")+".artifact.json", append(metadataBytes, '\n'), 0644))
	fmt.Printf("Success: %s\n", archivePath)
	fmt.Printf("Toolchain: %s (Windows 7 runtime acceptance remains a separate test)\n", toolchainVersion(*goBinary))
}

func defaultGoBinary() string {
	if value := strings.TrimSpace(os.Getenv("GO_BIN")); value != "" {
		return value
	}
	return "go"
}

func toolchainVersion(goBinary string) string {
	output, err := exec.Command(goBinary, "env", "GOVERSION").Output()
	if err != nil {
		fatalf("cannot query the Go toolchain %q: %v", goBinary, err)
	}
	return strings.TrimSpace(string(output))
}

// assertToolchain fails the build unless the toolchain still targets Windows 7.
// A Windows 7 incompatible sidecar is not a silent downgrade: it is a release
// blocker, because the plugin's whole reason to exist on that platform depends
// on it.
func assertToolchain(goBinary string, allowNonWin7 bool) {
	version := toolchainVersion(goBinary)
	if version == windows7GoPrefix {
		return
	}
	if allowNonWin7 {
		fmt.Fprintf(os.Stderr, "warning: building with %s; Windows 7 support is not guaranteed (-allow-non-win7)\n", version)
		return
	}
	fatalf("toolchain %s is not approved for this Windows 7 build; use %s or pass -allow-non-win7", version, windows7GoPrefix)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func fatalf(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(2)
}
func mustRead(name string) []byte {
	file, err := os.Open(name)
	must(err)
	defer file.Close()
	data, err := io.ReadAll(file)
	must(err)
	return data
}
func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
func mapArch(value string) string {
	if value == "x64" {
		return "amd64"
	}
	return value
}
func mapRuntimeArch(value string) string {
	if value == "amd64" {
		return "x64"
	}
	return value
}
