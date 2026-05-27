package rundmc

import (
	"archive/tar"
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/guardian/gardener"
	spec "code.cloudfoundry.org/guardian/gardener/container-spec"
	"code.cloudfoundry.org/guardian/rundmc/event"
	"code.cloudfoundry.org/guardian/rundmc/goci"
	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry/dropsonde/metrics"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate . Depot
//counterfeiter:generate . OCIRuntime
//counterfeiter:generate . NstarRunner
//counterfeiter:generate . EventStore
//counterfeiter:generate . ProcessesStopper
//counterfeiter:generate . StateStore
//counterfeiter:generate . PeaCreator
//counterfeiter:generate . PeaUsernameResolver
//counterfeiter:generate . RuntimeStopper
//counterfeiter:generate . CPUCgrouper

type Depot interface {
	Destroy(log lager.Logger, handle string) error
}

//counterfeiter:generate . BundleGenerator
type BundleGenerator interface {
	Generate(desiredContainerSpec spec.DesiredContainerSpec) (goci.Bndl, error)
}

type Status string

const RunningStatus Status = "running"
const CreatedStatus Status = "created"
const StoppedStatus Status = "stopped"

type State struct {
	Pid    int
	Status Status
}

type OCIRuntime interface {
	Create(log lager.Logger, id string, bundle goci.Bndl, io garden.ProcessIO) error
	Exec(log lager.Logger, id string, spec garden.ProcessSpec, io garden.ProcessIO) (garden.Process, error)
	Attach(log lager.Logger, id, processId string, io garden.ProcessIO) (garden.Process, error)
	Delete(log lager.Logger, id string) error
	State(log lager.Logger, id string) (State, error)
	Stats(log lager.Logger, id string) (gardener.StatsContainerMetrics, error)
	Events(log lager.Logger) (<-chan event.Event, error)
	ContainerHandles() ([]string, error)
	ContainerPeaHandles(log lager.Logger, id string) ([]string, error)
	BundleInfo(log lager.Logger, id string) (string, goci.Bndl, error)
	RemoveBundle(log lager.Logger, id string) error
}

type PeaCreator interface {
	CreatePea(log lager.Logger, processSpec garden.ProcessSpec, pio garden.ProcessIO, sandboxHandle string) (garden.Process, error)
}

type NstarRunner interface {
	StreamIn(log lager.Logger, pid int, path string, user string, tarStream io.Reader) error
	StreamOut(log lager.Logger, pid int, path string, user string) (io.ReadCloser, error)
}

type ProcessesStopper interface {
	StopAll(log lager.Logger, cgroupName string, save []int, kill bool) error
}

type RuntimeStopper interface {
	Stop() error
}

type EventStore interface {
	OnEvent(id string, event string) error
	Events(id string) []string
}

type StateStore interface {
	StoreStopped(handle string)
	IsStopped(handle string) bool
}

type PeaUsernameResolver interface {
	ResolveUser(log lager.Logger, handle string, image garden.ImageRef, username string) (int, int, error)
}

type CPUCgrouper interface {
	PrepareCgroups(handle string) error
	CleanupCgroups(handle string) error
	ReadTotalCgroupUsage(handle string, cpuStats garden.ContainerCPUStat) (garden.ContainerCPUStat, error)
}

// Containerizer knows how to manage a depot of container bundles
type Containerizer struct {
	depot                  Depot
	bundler                BundleGenerator
	runtime                OCIRuntime
	processesStopper       ProcessesStopper
	nstar                  NstarRunner
	events                 EventStore
	states                 StateStore
	peaCreator             PeaCreator
	peaUsernameResolver    PeaUsernameResolver
	cpuEntitlementPerShare float64
	runtimeStopper         RuntimeStopper
	cpuCgrouper            CPUCgrouper
	// Patch5-streamout-exec: runtime type for gVisor exec-based stream
	runtimeType string
}

func New(
	depot Depot,
	bundler BundleGenerator,
	runtime OCIRuntime,
	nstarRunner NstarRunner,
	processesStopper ProcessesStopper,
	events EventStore,
	states StateStore,
	peaCreator PeaCreator,
	peaUsernameResolver PeaUsernameResolver,
	cpuEntitlementPerShare float64,
	runtimeStopper RuntimeStopper,
	cpuCgrouper CPUCgrouper,
	runtimeType string,
) *Containerizer {
	containerizer := &Containerizer{
		depot:                  depot,
		bundler:                bundler,
		runtime:                runtime,
		nstar:                  nstarRunner,
		processesStopper:       processesStopper,
		events:                 events,
		states:                 states,
		peaCreator:             peaCreator,
		peaUsernameResolver:    peaUsernameResolver,
		cpuEntitlementPerShare: cpuEntitlementPerShare,
		runtimeStopper:         runtimeStopper,
		cpuCgrouper:            cpuCgrouper,
		runtimeType:            runtimeType,
	}
	return containerizer
}

func (c *Containerizer) WatchRuntimeEvents(log lager.Logger) error {
	events, err := c.runtime.Events(log)
	if err != nil {
		return err
	}

	go func() {
		for event := range events {
			if err := c.events.OnEvent(event.ContainerID, event.Message); err != nil {
				log.Error("failed to store event", err, lager.Data{"event": event})
			}
		}
	}()

	return nil
}

// Create creates a bundle in the depot and starts its init process
func (c *Containerizer) Create(log lager.Logger, spec spec.DesiredContainerSpec) error {
	log = log.Session("containerizer-create", lager.Data{"handle": spec.Handle})

	log.Info("start")
	defer log.Info("finished")

	bundle, err := c.bundler.Generate(spec)
	if err != nil {
		log.Error("bundle-generate-failed", err)
		return err
	}

	if err := c.cpuCgrouper.PrepareCgroups(spec.Handle); err != nil {
		log.Error("prepare-cgroups-failed", err)
		return err
	}

	if err := c.runtime.Create(log, spec.Handle, bundle, garden.ProcessIO{}); err != nil {
		log.Error("runtime-create-failed", err)
		return err
	}

	return nil
}

// Run runs a process inside a running container
func (c *Containerizer) Run(log lager.Logger, handle string, spec garden.ProcessSpec, io garden.ProcessIO) (garden.Process, error) {
	log = log.Session("run", lager.Data{"handle": handle, "path": spec.Path})

	log.Info("started")
	defer log.Info("finished")

	if isPea(spec) {
		if shouldResolveUsername(spec.User) {
			resolvedUID, resolvedGID, err := c.peaUsernameResolver.ResolveUser(log, handle, spec.Image, spec.User)
			if err != nil {
				return nil, err
			}

			spec.User = fmt.Sprintf("%d:%d", resolvedUID, resolvedGID)
		}

		return c.peaCreator.CreatePea(log, spec, io, handle)
	}

	if spec.BindMounts != nil {
		err := fmt.Errorf("Running a process with bind mounts and no image provided is not allowed")
		log.Error("invalid-spec", err)
		return nil, err
	}

	return c.runtime.Exec(log, handle, spec, io)
}

func isPea(spec garden.ProcessSpec) bool {
	return spec.Image != (garden.ImageRef{})
}

func shouldResolveUsername(username string) bool {
	return username != "" && !strings.Contains(username, ":")
}

func (c *Containerizer) Attach(log lager.Logger, handle string, processID string, io garden.ProcessIO) (garden.Process, error) {
	log = log.Session("attach", lager.Data{"handle": handle, "process-id": processID})

	log.Info("started")
	defer log.Info("finished")

	return c.runtime.Attach(log, handle, processID, io)
}

// StreamIn streams files in to the container
func (c *Containerizer) StreamIn(log lager.Logger, handle string, spec garden.StreamInSpec) error {
	log = log.Session("stream-in", lager.Data{"handle": handle})
	log.Info("started")
	defer log.Info("finished")

	defer func(startedAt time.Time) {
		_ = metrics.SendValue("StreamInDuration", float64(time.Since(startedAt).Nanoseconds()), "nanos")
	}(time.Now())

	state, err := c.runtime.State(log, handle)
	if err != nil {
		log.Error("check-pid-failed", err)
		return fmt.Errorf("stream-in: pid not found for container")
	}

	// Patch10-streamin-exec: for non-runc runtimes (gVisor), the host-side
	// rootfs extraction (Patch6) causes dentry cache inconsistency in the
	// sentry. Use runtime.Exec to run tar inside the container instead.
	procPasswd := filepath.Join("/proc", strconv.Itoa(state.Pid), "root", "etc", "passwd")
	if _, statErr := os.Stat(procPasswd); statErr != nil {
		return c.streamInViaExec(log, handle, spec)
	}
	if err := c.nstar.StreamIn(log, state.Pid, spec.Path, spec.User, spec.TarStream); err != nil {
		log.Error("nstar-failed", err)
		return fmt.Errorf("stream-in: nstar: %s", err)
	}

	return nil
}

// StreamOut stream files from the container
func (c *Containerizer) StreamOut(log lager.Logger, handle string, spec garden.StreamOutSpec) (io.ReadCloser, error) {
	log = log.Session("stream-out", lager.Data{"handle": handle})

	log.Info("started")
	defer log.Info("finished")

	// Patch5-streamout-exec: for gVisor, use runtime exec instead of nstar.
	// nstar uses nsenter which doesn't see gVisor's virtual filesystem.
	if c.runtimeType != "" && c.runtimeType != "io.containerd.runc.v2" {
		return c.streamOutViaExec(log, handle, spec)
	}

	state, err := c.runtime.State(log, handle)
	if err != nil {
		log.Error("check-pid-failed", err)
		return nil, fmt.Errorf("stream-out: pid not found for container")
	}

	stream, err := c.nstar.StreamOut(log, state.Pid, spec.Path, spec.User)
	if err != nil {
		log.Error("nstar-failed", err)
		return nil, fmt.Errorf("stream-out: nstar: %s", err)
	}

	return stream, nil
}

// Stop stops all the processes other than the init process in the container
func (c *Containerizer) Stop(log lager.Logger, handle string, kill bool) error {
	log = log.Session("stop", lager.Data{"handle": handle, "kill": kill})

	log.Info("started")
	defer log.Info("finished")

	state, err := c.runtime.State(log, handle)
	if err != nil {
		log.Error("check-pid-failed", err)
		return fmt.Errorf("stop: pid not found for container: %s", err)
	}

	if err = c.processesStopper.StopAll(log, handle, []int{state.Pid}, kill); err != nil {
		log.Error("stop-all-processes-failed", err, lager.Data{"pid": state.Pid})
		return fmt.Errorf("stop: %s", err)
	}

	c.states.StoreStopped(handle)
	return nil
}

// Destroy deletes the container and the bundle directory
func (c *Containerizer) Destroy(log lager.Logger, handle string) error {
	log = log.Session("destroy", lager.Data{"handle": handle})

	log.Info("started")
	defer log.Info("finished")

	if err := c.runtime.Delete(log, handle); err != nil {
		return err
	}

	return c.cpuCgrouper.CleanupCgroups(handle)
}

func (c *Containerizer) RemoveBundle(log lager.Logger, handle string) error {
	log = log.Session("remove-bundle", lager.Data{"handle": handle})
	// TODO: this should be removed once containerd processes are the only code path
	// and this should be managed by the network depot
	if err := c.depot.Destroy(log, handle); err != nil {
		log.Debug("failed-to-remove-bundle-dir")
	}
	return c.runtime.RemoveBundle(log, handle)
}

func (c *Containerizer) Info(log lager.Logger, handle string) (spec.ActualContainerSpec, error) {
	bundlePath, bundle, err := c.runtime.BundleInfo(log, handle)
	if err != nil {
		return spec.ActualContainerSpec{}, err
	}

	state, err := c.runtime.State(log, handle)
	if err != nil {
		return spec.ActualContainerSpec{}, err
	}

	privileged := true
	for _, ns := range bundle.Namespaces() {
		if ns.Type == specs.UserNamespace {
			privileged = false
			break
		}
	}

	var cpuShares, limitInBytes uint64
	if bundle.Resources() != nil {
		if bundle.Resources().CPU != nil {
			cpuShares = *bundle.Resources().CPU.Shares
		}
		if cpuWeight, ok := bundle.Resources().Unified["cpu.weight"]; ok {
			cpuSharesInt, err := strconv.Atoi(cpuWeight)
			if err != nil {
				return spec.ActualContainerSpec{}, err
			}
			cpuShares = uint64(cpuSharesInt)
		}
		if bundle.Resources().Memory != nil {
			// #nosec G115 - limits should never be negative
			limitInBytes = uint64(*bundle.Resources().Memory.Limit)
		}
		if memoryMax, ok := bundle.Resources().Unified["memory.max"]; ok {
			limitInBytesInt, err := strconv.Atoi(memoryMax)
			if err != nil {
				return spec.ActualContainerSpec{}, err
			}
			limitInBytes = uint64(limitInBytesInt)
		}
	} else {
		log.Debug("bundle-resources-is-nil", lager.Data{"bundle": bundle})
	}

	return spec.ActualContainerSpec{
		Pid:        state.Pid,
		BundlePath: bundlePath,
		RootFSPath: bundle.RootFS(),
		Events:     c.events.Events(handle),
		Stopped:    c.states.IsStopped(handle),
		Limits: garden.Limits{
			CPU: garden.CPULimits{
				LimitInShares: cpuShares,
			},
			Memory: garden.MemoryLimits{
				LimitInBytes: limitInBytes,
			},
		},
		Privileged: privileged,
	}, nil
}

func (c *Containerizer) Metrics(log lager.Logger, handle string) (gardener.ActualContainerMetrics, error) {
	containerMetrics, err := c.runtime.Stats(log, handle)
	if err != nil {
		return gardener.ActualContainerMetrics{}, err
	}

	totalCPUUsage, err := c.cpuCgrouper.ReadTotalCgroupUsage(handle, containerMetrics.CPU)
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "no such file or directory") {
			totalCPUUsage = containerMetrics.CPU
		} else {
			return gardener.ActualContainerMetrics{}, err
		}
	}

	containerMetrics.CPU = garden.ContainerCPUStat{
		Usage:  totalCPUUsage.Usage,
		User:   totalCPUUsage.User,
		System: totalCPUUsage.System,
	}

	actualContainerMetrics := gardener.ActualContainerMetrics{
		StatsContainerMetrics: containerMetrics,
	}

	_, bundle, err := c.runtime.BundleInfo(log, handle)
	if isNotFound(err) { // pea
		return actualContainerMetrics, nil
	}
	if err != nil {
		return gardener.ActualContainerMetrics{}, err
	}

	actualContainerMetrics.CPUEntitlement = calculateCPUEntitlement(getShares(bundle), c.cpuEntitlementPerShare, containerMetrics.Age)

	return actualContainerMetrics, nil
}

func (c *Containerizer) Shutdown() error {
	return c.runtimeStopper.Stop()
}

func isNotFound(err error) bool {
	_, ok := err.(garden.ContainerNotFoundError)
	return ok
}

func (c *Containerizer) Handles() ([]string, error) {
	return c.runtime.ContainerHandles()
}

func calculateCPUEntitlement(shares uint64, entitlementPerShare float64, containerAge time.Duration) uint64 {
	return uint64(float64(shares) * (entitlementPerShare / 100) * float64(containerAge.Nanoseconds()))
}

func getShares(bundle goci.Bndl) uint64 {
	resources := bundle.Resources()
	if resources == nil {
		return 0
	}
	cpu := resources.CPU
	if cpu == nil {
		if cpuWeight, ok := resources.Unified["cpu.weight"]; ok {
			cpuWeightUint, err := strconv.ParseUint(cpuWeight, 10, 64)
			if err != nil {
				return 0
			}
			return ConvertCgroupV2ValueToCPUShares(cpuWeightUint)
		}

		return 0
	}
	shares := cpu.Shares
	if shares == nil {
		return 0
	}
	return *shares
}

// Patch5-streamout-exec: exec-based StreamOut for non-runc runtimes (gVisor).
func (c *Containerizer) streamOutViaExec(log lager.Logger, handle string, spec garden.StreamOutSpec) (io.ReadCloser, error) {
	path := spec.Path
	sourcePath := filepath.Dir(path)
	compressPath := filepath.Base(path)
	if strings.HasSuffix(path, "/") {
		sourcePath = path
		compressPath = "."
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	processSpec := garden.ProcessSpec{
		Path: "/bin/tar",
		Args: []string{"-cf", "-", "-C", sourcePath, compressPath},
		User: spec.User,
	}
	if processSpec.User == "" {
		processSpec.User = "root"
	}

	processIO := garden.ProcessIO{
		Stdout: writer,
		Stderr: io.Discard,
	}

	process, err := c.runtime.Exec(log, handle, processSpec, processIO)
	if err != nil {
		writer.Close()
		reader.Close()
		log.Error("exec-tar-failed", err)
		return nil, fmt.Errorf("stream-out: exec tar: %s", err)
	}

	go func() {
		process.Wait()
		writer.Close()
	}()

	return reader, nil
}

// Patch10-streamin-exec: exec-based StreamIn for non-runc runtimes (gVisor).
// Runs tar inside the container through the proper exec path so the sentry's
// dentry cache stays consistent with what's on disk.
func (c *Containerizer) streamInViaExec(log lager.Logger, handle string, spec garden.StreamInSpec) error {
	user := spec.User
	if user == "" {
		user = "root"
	}

	processSpec := garden.ProcessSpec{
		Path: "/bin/tar",
		Args: []string{"-xf", "-", "-C", spec.Path},
		User: user,
	}

	processIO := garden.ProcessIO{
		Stdin:  spec.TarStream,
		Stdout: io.Discard,
		Stderr: io.Discard,
	}

	log.Debug("stream-in-via-exec", lager.Data{"handle": handle, "path": spec.Path, "user": user})

	process, err := c.runtime.Exec(log, handle, processSpec, processIO)
	if err != nil {
		log.Error("exec-tar-failed", err)
		return fmt.Errorf("stream-in: exec tar: %s", err)
	}

	exitCode, err := process.Wait()
	if err != nil {
		return fmt.Errorf("stream-in: exec tar wait: %s", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("stream-in: exec tar exited %d", exitCode)
	}

	return nil
}

// Patch6-bundle-rootfs-streamin: helper that extracts a tar stream directly
// into bundle.Spec.Root.Path (the host-side rootfs prepared by grootfs),
// applying the container uid directly (no offset — grootfs uses idmap mount).
// Used as fallback when exec-based streaming is not available.
func (c *Containerizer) streamInToBundleRoot(log lager.Logger, handle string, spec garden.StreamInSpec) error {
	_, bundle, err := c.runtime.BundleInfo(log, handle)
	if err != nil {
		log.Error("bundle-info-failed", err)
		return fmt.Errorf("stream-in: bundle info: %s", err)
	}
	if bundle.Spec.Root == nil || bundle.Spec.Root.Path == "" {
		return fmt.Errorf("stream-in: bundle has no root path")
	}
	rootfs := bundle.Spec.Root.Path

	user := spec.User
	if user == "" {
		user = "root"
	}

	ctrUID, ctrGID, err := lookupUIDGIDFromPasswd(filepath.Join(rootfs, "etc", "passwd"), filepath.Join(rootfs, "etc", "group"), user)
	if err != nil {
		return fmt.Errorf("stream-in: user lookup: %s", err)
	}

	// Patch8-no-uid-offset: grootfs leaves on-disk uids unshifted; the user-NS
	// idmap mount applies the shift at runtime. Existing rootfs files (e.g.
	// /home/vcap) are owned by raw in-image uid 2000. Match that — write with
	// container-side uid/gid directly so apps see their files as vcap-owned.
	hostUID := ctrUID
	hostGID := ctrGID

	dest := filepath.Join(rootfs, spec.Path)
	// Patch7-rootfs-symlink-resolve: dest itself may be a symlink (e.g. ./app -> /home/vcap/app
	// in cflinuxfs4). Resolve before extracting.
	destResolved, err := resolveInRootfs(rootfs, dest)
	if err != nil {
		return fmt.Errorf("stream-in: resolve dest: %s", err)
	}
	dest = destResolved
	if fi, err := os.Lstat(dest); err != nil {
		if err := os.MkdirAll(dest, 0755); err != nil {
			return fmt.Errorf("stream-in: mkdir dest: %s", err)
		}
	} else if !fi.IsDir() {
		return fmt.Errorf("stream-in: dest %s is not a directory", dest)
	}

	tr := tar.NewReader(spec.TarStream)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream-in: tar read: %s", err)
		}
		targetRaw := filepath.Join(dest, hdr.Name)
		cleanedTarget := filepath.Clean(targetRaw)
		cleanedDest := filepath.Clean(dest)
		// Patch9-skip-dot-entry: tar entries with name "./" or "" map to dest
		// itself — we already created/validated dest at the top of the func.
		if cleanedTarget == cleanedDest {
			continue
		}
		// guard against path escapes via .. components in tar entries
		if !strings.HasPrefix(cleanedTarget, filepath.Clean(rootfs)) {
			return fmt.Errorf("stream-in: tar entry escapes rootfs: %s", hdr.Name)
		}
		// Resolve any pre-existing symlink components in the parent path
		// against rootfs (so writes follow symlinks like /app -> /home/vcap/app
		// while still being confined to the rootfs).
		parent, err := resolveInRootfs(rootfs, filepath.Dir(targetRaw))
		if err != nil {
			return fmt.Errorf("stream-in: resolve parent of %s: %s", hdr.Name, err)
		}
		target := filepath.Join(parent, filepath.Base(targetRaw))
		switch hdr.Typeflag {
		case tar.TypeDir:
			if fi, err := os.Lstat(target); err == nil {
				if fi.IsDir() {
					_ = os.Lchown(target, hostUID, hostGID)
					continue
				}
				if fi.Mode()&os.ModeSymlink != 0 {
					// Symlink in rootfs (e.g. /app); leave it alone, follow it.
					resolved, rerr := resolveInRootfs(rootfs, target)
					if rerr == nil {
						if rfi, serr := os.Stat(resolved); serr == nil && rfi.IsDir() {
							continue
						}
						_ = os.MkdirAll(resolved, os.FileMode(hdr.Mode)&os.ModePerm)
						_ = os.Lchown(resolved, hostUID, hostGID)
						continue
					}
				}
			}
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&os.ModePerm); err != nil {
				return fmt.Errorf("stream-in: mkdir %s: %s", target, err)
			}
			_ = os.Lchown(target, hostUID, hostGID)
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("stream-in: mkdir parent of %s: %s", target, err)
			}
			f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode)&os.ModePerm)
			if err != nil {
				return fmt.Errorf("stream-in: open %s: %s", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("stream-in: copy %s: %s", target, err)
			}
			f.Close()
			_ = os.Lchown(target, hostUID, hostGID)
		case tar.TypeSymlink:
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return fmt.Errorf("stream-in: symlink %s: %s", target, err)
			}
			_ = os.Lchown(target, hostUID, hostGID)
		case tar.TypeLink:
			linkRaw := filepath.Join(dest, hdr.Linkname)
			linkTarget, lerr := resolveInRootfs(rootfs, linkRaw)
			if lerr != nil {
				return fmt.Errorf("stream-in: resolve hardlink target %s: %s", hdr.Linkname, lerr)
			}
			_ = os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return fmt.Errorf("stream-in: hardlink %s: %s", target, err)
			}
		default:
			// skip devices, fifos, etc.
		}
	}
	log.Info("bundle-rootfs-streamin-done", lager.Data{"rootfs": rootfs, "dest": spec.Path, "user": user, "hostUID": hostUID, "hostGID": hostGID})
	return nil
}

// lookupUIDGIDFromPasswd resolves a user spec ("vcap" or "uid:gid") against the
// supplied passwd/group files. Used by Patch6 to determine container-side uid/gid.
func lookupUIDGIDFromPasswd(passwdPath, groupPath, user string) (int, int, error) {
	// Allow numeric "uid:gid" form (used by Garden's process spec sometimes).
	if strings.Contains(user, ":") {
		parts := strings.SplitN(user, ":", 2)
		u, err1 := strconv.Atoi(parts[0])
		g, err2 := strconv.Atoi(parts[1])
		if err1 == nil && err2 == nil {
			return u, g, nil
		}
	}
	f, err := os.Open(passwdPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, ":")
		if len(fields) >= 4 && fields[0] == user {
			u, err1 := strconv.Atoi(fields[2])
			g, err2 := strconv.Atoi(fields[3])
			if err1 != nil || err2 != nil {
				return 0, 0, fmt.Errorf("invalid passwd entry for %s", user)
			}
			return u, g, nil
		}
	}
	return 0, 0, fmt.Errorf("user %s not found in %s", user, passwdPath)
}

// resolveInRootfs walks `path` component-by-component, resolving any symlinks
// found, but keeping the final result confined to `rootfs`. Absolute symlinks
// are interpreted relative to `rootfs` (so `/home/vcap` inside rootfs resolves
// to `<rootfs>/home/vcap`). Used by Patch6/Patch7 to follow rootfs symlinks
// (e.g. `/app -> /home/vcap/app`) while preventing escape.
func resolveInRootfs(rootfs, path string) (string, error) {
	rootfs = filepath.Clean(rootfs)
	cleaned := filepath.Clean(path)
	if !strings.HasPrefix(cleaned, rootfs) {
		return "", fmt.Errorf("path %s outside rootfs %s", cleaned, rootfs)
	}
	rel := strings.TrimPrefix(cleaned, rootfs)
	rel = strings.TrimPrefix(rel, "/")
	parts := []string{}
	if rel != "" {
		parts = strings.Split(rel, "/")
	}
	cur := rootfs
	for i := 0; i < 64; i++ { // bound symlink chain depth
		if len(parts) == 0 {
			return cur, nil
		}
		next := filepath.Join(cur, parts[0])
		parts = parts[1:]
		fi, err := os.Lstat(next)
		if err != nil {
			// path doesn't exist yet; just join the rest.
			return filepath.Join(append([]string{next}, parts...)...), nil
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			cur = next
			continue
		}
		tgt, err := os.Readlink(next)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(tgt) {
			cur = rootfs
			tgtRel := strings.TrimPrefix(filepath.Clean(tgt), "/")
			newParts := []string{}
			if tgtRel != "" {
				newParts = strings.Split(tgtRel, "/")
			}
			parts = append(newParts, parts...)
		} else {
			newParts := strings.Split(filepath.Clean(tgt), "/")
			parts = append(newParts, parts...)
		}
	}
	return "", fmt.Errorf("symlink chain too deep resolving %s", path)
}
