package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/yaml"
)

func main() {
	var kubeconfig, outDir, archiveTo string
	var parentOnly bool
	var parallelism int

	defaultKC := ""
	if home := homedir.HomeDir(); home != "" {
		defaultKC = filepath.Join(home, ".kube", "config")
	}
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKC, "path to kubeconfig")
	flag.StringVar(&outDir, "out", "dump", "output directory")
	flag.StringVar(&archiveTo, "archive-to", "", "if set, tar.gz the dump into this directory and remove the source")
	flag.BoolVar(&parentOnly, "parent-only", false, "skip resources that have ownerReferences")
	flag.IntVar(&parallelism, "p", 32, "parallel workers")
	flag.Parse()

	if parallelism < 1 {
		log.Fatalf("-p must be at least 1, got %d", parallelism)
	}
	outDir = filepath.Clean(outDir)
	if archiveTo != "" {
		inside, err := isWithin(outDir, archiveTo)
		if err != nil {
			log.Fatal(err)
		}
		if inside {
			log.Fatalf("-archive-to %q must not be inside -out %q (the source is removed after archiving)", archiveTo, outDir)
		}
	}

	// Refuse to mix a new dump with leftovers: files for objects deleted since
	// an earlier run would otherwise linger and be re-applied or archived.
	if entries, err := os.ReadDir(outDir); err == nil {
		if len(entries) > 0 {
			log.Fatalf("output directory %q is not empty; remove it or choose another -out", outDir)
		}
	} else if !os.IsNotExist(err) {
		log.Fatalf("output directory %q: %v", outDir, err)
	}

	// failures counts everything that makes the dump incomplete; any failure
	// results in a non-zero exit status.
	var failures atomic.Int64

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	cfg.QPS = 1000
	cfg.Burst = 2000

	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	lists, err := disco.ServerPreferredResources()
	if err != nil {
		// partial discovery can return a non-nil error with usable lists
		if !discovery.IsGroupDiscoveryFailedError(err) || len(lists) == 0 {
			log.Fatalf("discovery: %v", err)
		}
		log.Printf("discovery: %v", err)
		failures.Add(1)
	}

	type job struct {
		gvr        schema.GroupVersionResource
		namespaced bool
		label      string // resource name used in filenames and logs
	}
	var jobs []job
	for _, l := range lists {
		if l == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(l.GroupVersion)
		if err != nil {
			continue
		}
		for _, r := range l.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			if !slices.Contains(r.Verbs, "list") {
				continue
			}
			if r.Name == "events" || r.Kind == "Event" {
				continue
			}
			jobs = append(jobs, job{
				gvr:        schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name},
				namespaced: r.Namespaced,
			})
		}
	}

	// The same plural can exist in several groups (e.g. core "nodes" and
	// metrics.k8s.io "nodes"). Qualify non-core ones with their group so their
	// files do not overwrite each other.
	groupsByName := map[string]int{}
	for _, j := range jobs {
		groupsByName[j.gvr.Resource]++
	}
	for i := range jobs {
		jobs[i].label = jobs[i].gvr.Resource
		if groupsByName[jobs[i].gvr.Resource] > 1 && jobs[i].gvr.Group != "" {
			jobs[i].label += "." + jobs[i].gvr.Group
		}
	}

	var dirsMade sync.Map
	ensureDir := func(d string) error {
		if _, ok := dirsMade.Load(d); ok {
			return nil
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		dirsMade.Store(d, struct{}{})
		return nil
	}
	if err := ensureDir(outDir); err != nil {
		log.Fatal(err)
	}

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	var total atomic.Int64

	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()

			// ResourceVersion="0" allows the apiserver to serve from its watch cache
			// instead of forcing a quorum read against etcd. Slight staleness is fine
			// for a manifest dump and dramatically reduces apiserver+etcd load.
			list, err := dyn.Resource(j.gvr).List(context.TODO(), metav1.ListOptions{ResourceVersion: "0"})
			if err != nil {
				log.Printf("list %s: %v", j.label, err)
				failures.Add(1)
				return
			}
			written := 0
			for i := range list.Items {
				item := &list.Items[i]
				if parentOnly && len(item.GetOwnerReferences()) > 0 {
					continue
				}
				var dir string
				if j.namespaced {
					ns := item.GetNamespace()
					if ns == "" {
						ns = "_default"
					}
					dir = filepath.Join(outDir, ns)
				} else {
					dir = filepath.Join(outDir, "_cluster")
				}
				if err := ensureDir(dir); err != nil {
					log.Printf("mkdir %s: %v", dir, err)
					failures.Add(1)
					continue
				}

				clean(item)

				path := filepath.Join(dir, fmt.Sprintf("%s_%s.yaml", j.label, fileSafe(item.GetName())))
				data, err := yaml.Marshal(item.Object)
				if err != nil {
					log.Printf("marshal %s: %v", path, err)
					failures.Add(1)
					continue
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					log.Printf("write %s: %v", path, err)
					failures.Add(1)
					continue
				}
				written++
			}
			total.Add(int64(written))
			log.Printf("%-40s %d items", j.label, written)
		}(j)
	}
	wg.Wait()
	log.Printf("done: %d files written to %s", total.Load(), outDir)

	if archiveTo != "" {
		archivePath, err := archive(outDir, archiveTo)
		if err != nil {
			log.Fatalf("archive: %v", err)
		}
		if err := os.RemoveAll(outDir); err != nil {
			log.Printf("remove source: %v", err)
			failures.Add(1)
		}
		log.Printf("archived to %s", archivePath)
	}

	if n := failures.Load(); n > 0 {
		log.Printf("dump is incomplete: %d error(s), see log above", n)
		os.Exit(1)
	}
}

// isWithin reports whether path is dir itself or located below it.
func isWithin(dir, path string) (bool, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return false, nil // e.g. different volumes on Windows
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))), nil
}

// fileSafe makes an object name usable as a filename on every OS. Names like
// "system:controller:job-controller" are valid in Kubernetes but ':' is not
// allowed on Windows. '%' cannot appear in object names, so the escape is
// collision-free.
func fileSafe(name string) string {
	if runtime.GOOS == "windows" {
		return strings.ReplaceAll(name, ":", "%3A")
	}
	return name
}

func archive(srcDir, destDir string) (archivePath string, err error) {
	if err = os.MkdirAll(destDir, 0o755); err != nil {
		return "", err
	}
	ts := time.Now().Format("20060102-150405")
	archivePath = filepath.Join(destDir, filepath.Base(srcDir)+"-"+ts+".tar.gz")

	f, err := os.Create(archivePath)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	gz := gzip.NewWriter(f)
	defer func() {
		if cerr := gz.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	tw := tar.NewWriter(gz)
	defer func() {
		if cerr := tw.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	base := filepath.Dir(srcDir)
	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if d.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
	return archivePath, err
}

func clean(u *unstructured.Unstructured) {
	m := u.Object

	for _, k := range []string{
		"creationTimestamp", "deletionGracePeriodSeconds", "deletionTimestamp",
		"finalizers", "generation", "managedFields", "ownerReferences",
		"resourceVersion", "selfLink", "uid",
	} {
		unstructured.RemoveNestedField(m, "metadata", k)
	}
	unstructured.RemoveNestedField(m, "status")

	ann := u.GetAnnotations()
	delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
	if len(ann) == 0 {
		unstructured.RemoveNestedField(m, "metadata", "annotations")
	} else {
		u.SetAnnotations(ann)
	}

	switch u.GetKind() {
	case "Service":
		fields := []string{"ipFamilies", "ipFamilyPolicy", "healthCheckNodePort"}
		// clusterIP: None marks a headless Service and must survive re-apply.
		if ip, _, _ := unstructured.NestedString(m, "spec", "clusterIP"); ip != "None" {
			fields = append(fields, "clusterIP", "clusterIPs")
		}
		for _, k := range fields {
			unstructured.RemoveNestedField(m, "spec", k)
		}
		if ports, ok, _ := unstructured.NestedSlice(m, "spec", "ports"); ok {
			for _, p := range ports {
				if pm, ok := p.(map[string]any); ok {
					delete(pm, "nodePort")
				}
			}
			if err := unstructured.SetNestedSlice(m, ports, "spec", "ports"); err != nil {
				log.Printf("clean Service ports: %v", err)
			}
		}
	case "ServiceAccount":
		unstructured.RemoveNestedField(m, "secrets")
	case "PersistentVolumeClaim":
		unstructured.RemoveNestedField(m, "spec", "volumeName")
	case "PersistentVolume":
		unstructured.RemoveNestedField(m, "spec", "claimRef")
	case "Node":
		for _, k := range []string{"podCIDR", "podCIDRs", "providerID"} {
			unstructured.RemoveNestedField(m, "spec", k)
		}
	}
}
