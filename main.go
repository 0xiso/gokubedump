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
		log.Printf("discovery warning: %v", err)
	}

	type job struct {
		gvr        schema.GroupVersionResource
		namespaced bool
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
				log.Printf("list %s: %v", j.gvr.Resource, err)
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
					continue
				}

				clean(item)

				path := filepath.Join(dir, fmt.Sprintf("%s_%s.yaml", j.gvr.Resource, item.GetName()))
				data, err := yaml.Marshal(item.Object)
				if err != nil {
					log.Printf("marshal %s: %v", path, err)
					continue
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					log.Printf("write %s: %v", path, err)
					continue
				}
				written++
			}
			total.Add(int64(written))
			log.Printf("%-40s %d items", j.gvr.Resource, written)
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
		}
		log.Printf("archived to %s", archivePath)
	}
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
		hdr.Name = rel
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
		for _, k := range []string{"clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy", "healthCheckNodePort"} {
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
