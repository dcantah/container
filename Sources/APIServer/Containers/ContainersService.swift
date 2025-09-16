//===----------------------------------------------------------------------===//
// Copyright © 2025 Apple Inc. and the container project authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//===----------------------------------------------------------------------===//

import CVersion
import ContainerClient
import ContainerPlugin
import ContainerSandboxService
import ContainerXPC
import Containerization
import ContainerizationError
import ContainerizationExtras
import ContainerizationOCI
import ContainerizationOS
import Foundation
import Logging

actor ContainersService {
    struct ContainerState {
        var snapshot: ContainerSnapshot
        var client: SandboxClient?

        func getClient() throws -> SandboxClient {
            guard let client else {
                throw ContainerizationError(.invalidState, message: "no sandbox client exists")
            }
            return client
        }
    }

    private static let machServicePrefix = "com.apple.container"
    private static let launchdDomainString = try! ServiceManager.getDomainString()

    private let log: Logger
    private let containerRoot: URL
    private let pluginLoader: PluginLoader
    private let runtimePlugins: [Plugin]

    private let lock = AsyncLock()
    private var containers: [String: ContainerState]

    public init(appRoot: URL, pluginLoader: PluginLoader, log: Logger) throws {
        let containerRoot = appRoot.appendingPathComponent("containers")
        try FileManager.default.createDirectory(at: containerRoot, withIntermediateDirectories: true)
        self.containerRoot = containerRoot
        self.pluginLoader = pluginLoader
        self.log = log
        self.runtimePlugins = pluginLoader.findPlugins().filter { $0.hasType(.runtime) }
        self.containers = try Self.loadAtBoot(root: containerRoot, loader: pluginLoader, log: log)
    }

    static func loadAtBoot(root: URL, loader: PluginLoader, log: Logger) throws -> [String: ContainerState] {
        var directories = try FileManager.default.contentsOfDirectory(
            at: root,
            includingPropertiesForKeys: [.isDirectoryKey]
        )
        directories = directories.filter {
            $0.isDirectory
        }

        let runtimePlugins = loader.findPlugins().filter { $0.hasType(.runtime) }
        var results = [String: ContainerState]()
        for dir in directories {
            do {
                let bundle = ContainerClient.Bundle(path: dir)
                let config = try bundle.configuration
                let state = ContainerState(
                    snapshot: .init(
                        configuration: config,
                        status: .stopped,
                        networks: []
                    )
                )
                results[config.id] = state
                let plugin = runtimePlugins.first { $0.name == config.runtimeHandler }
                guard let plugin else {
                    throw ContainerizationError(
                        .internalError,
                        message: "Failed to find runtime plugin \(config.runtimeHandler)"
                    )
                }
                try Self.registerService(
                    plugin: plugin,
                    loader: loader,
                    configuration: config,
                    path: dir
                )
            } catch {
                try? FileManager.default.removeItem(at: dir)
                log.warning("failed to load container bundle at \(dir.path)")
            }
        }
        return results
    }

    private func setContainer(_ id: String, _ item: ContainerState, context: AsyncLock.Context) async {
        self.containers[id] = item
    }

    /// List all containers registered with the service.
    public func list() async throws -> [ContainerSnapshot] {
        self.log.debug("\(#function)")
        return self.containers.values.map { $0.snapshot }
    }

    /// Execute an operation with the current container list while maintaining atomicity
    /// This prevents race conditions where containers are created during the operation
    public func withContainerList<T: Sendable>(_ operation: @Sendable @escaping ([ContainerSnapshot]) async throws -> T) async throws -> T {
        try await lock.withLock { context in
            let snapshots = await self.containers.values.map { $0.snapshot }
            return try await operation(snapshots)
        }
    }

    /// Create a new container from the provided id and configuration.
    public func create(configuration: ContainerConfiguration, kernel: Kernel, options: ContainerCreateOptions) async throws {
        self.log.debug("\(#function)")

        guard containers[configuration.id] == nil else {
            throw ContainerizationError(
                .exists,
                message: "container already exists: \(configuration.id)"
            )
        }

        var allHostnames = Set<String>()
        for container in containers.values {
            for attachmentConfiguration in container.snapshot.configuration.networks {
                allHostnames.insert(attachmentConfiguration.options.hostname)
            }
        }

        var conflictingHostnames = [String]()
        for attachmentConfiguration in configuration.networks {
            if allHostnames.contains(attachmentConfiguration.options.hostname) {
                conflictingHostnames.append(attachmentConfiguration.options.hostname)
            }
        }

        guard conflictingHostnames.isEmpty else {
            throw ContainerizationError(
                .exists,
                message: "hostname(s) already exist: \(conflictingHostnames)"
            )
        }

        let runtimePlugin = self.runtimePlugins.filter {
            $0.name == configuration.runtimeHandler
        }.first
        guard let runtimePlugin else {
            throw ContainerizationError(
                .notFound,
                message: "unable to locate runtime plugin \(configuration.runtimeHandler)"
            )
        }

        let path = self.containerRoot.appendingPathComponent(configuration.id)
        let systemPlatform = kernel.platform
        let initFs = try await getInitBlock(for: systemPlatform.ociPlatform())

        let bundle = try ContainerClient.Bundle.create(
            path: path,
            initialFilesystem: initFs,
            kernel: kernel,
            containerConfiguration: configuration
        )
        do {
            let containerImage = ClientImage(description: configuration.image)
            let imageFs = try await containerImage.getCreateSnapshot(platform: configuration.platform)
            try bundle.setContainerRootFs(cloning: imageFs)
            try bundle.write(filename: "options.json", value: options)

            try Self.registerService(
                plugin: runtimePlugin,
                loader: self.pluginLoader,
                configuration: configuration,
                path: path
            )

            let snapshot = ContainerSnapshot(
                configuration: configuration,
                status: .stopped,
                networks: []
            )
            self.containers[configuration.id] = ContainerState(snapshot: snapshot)
        } catch {
            do {
                try bundle.delete()
            } catch {
                self.log.error("failed to delete bundle for container \(configuration.id): \(error)")
            }
            throw error
        }
    }

    public func handleContainerEvents(event: ContainerEvent) async throws {
        self.log.debug("\(#function)")
        await self.lock.withLock { context in
            switch event {
            case .containerExit(let id, let code):
                await self.containerProcessExitHandler(id, Int32(code), context: context)
            }
        }
    }

    /// Bootstrap the init process of the container.
    public func bootstrap(id: String, stdio: [FileHandle?]) async throws {
        self.log.debug("\(#function)")
        do {
            try await self.lock.withLock { context in
                var item = try await self.get(id: id, context: context)
                let runtime = item.snapshot.configuration.runtimeHandler
                let sandboxClient = try await SandboxClient.create(
                    id: id,
                    runtime: runtime
                )
                try await sandboxClient.bootstrap(stdio: stdio)
                item.client = sandboxClient
                await self.setContainer(id, item, context: context)
            }
        } catch {
            do {
                try _cleanup(id: id)
            } catch {
                self.log.error("failed to cleanup container \(id) after bootstrap failure: \(error)")
            }
            throw error
        }
    }

    /// Create a new process in the container.
    public func createProcess(
        id: String,
        processID: String,
        config: ProcessConfiguration,
        stdio: [FileHandle?]
    ) async throws {
        self.log.debug("\(#function)")

        let item = try self._get(id: id)
        do {
            let client = try item.getClient()
            try await client.createProcess(
                processID,
                config: config,
                stdio: stdio
            )
        } catch {
            do {
                try _cleanup(id: id)
            } catch {
                self.log.error("failed to cleanup container \(id) after start failure: \(error)")
            }
            throw error
        }
    }

    /// Start a process in a container. This can either be a process created via
    /// createProcess, or the init process of the container which requires
    /// id == processID.
    public func startProcess(id: String, processID: String) async throws {
        self.log.debug("\(#function)")

        do {
            try await self.lock.withLock { context in
                var item = try await self.get(id: id, context: context)
                let client = try item.getClient()
                try await client.startProcess(processID)

                if id == processID {
                    let sandboxSnapshot = try await client.state()
                    item.snapshot.status = .running
                    item.snapshot.networks = sandboxSnapshot.networks
                    await self.setContainer(id, item, context: context)
                }
            }
        } catch {
            do {
                try _cleanup(id: id)
            } catch {
                self.log.error("failed to cleanup container \(id) after start failure: \(error)")
            }
            throw error
        }
    }

    /// Send a signal to the container.
    public func kill(id: String, processID: String, signal: Int64) async throws {
        self.log.debug("\(#function)")

        let item = try self._get(id: id)
        let client = try item.getClient()
        try await client.kill(processID, signal: signal)
    }

    /// Stop all containers inside the sandbox, aborting any processes currently
    /// executing inside the container, before stopping the underlying sandbox.
    public func stop(id: String, options: ContainerStopOptions) async throws {
        self.log.debug("\(#function)")

        let item = try self._get(id: id)
        let client = try item.getClient()
        try await client.stop(options: options)
    }

    /// Wait waits for the container's init process or exec to exit and returns the
    /// exit status.
    public func wait(id: String, processID: String) async throws -> Int32 {
        self.log.debug("\(#function)")

        let item = try self._get(id: id)
        let client = try item.getClient()
        return try await client.wait(processID)
    }

    /// Resize resizes the container's PTY if one exists.
    public func resize(id: String, processID: String, size: Terminal.Size) async throws {
        self.log.debug("\(#function)")

        let item = try self._get(id: id)
        let client = try item.getClient()
        try await client.resize(processID, size: size)
    }

    // Get the logs for the container.
    public func logs(id: String) async throws -> [FileHandle] {
        self.log.debug("\(#function)")

        // Logs doesn't care if the container is running or not, just that
        // the bundle is there, and that the files actually exist.
        do {
            let path = self.containerRoot.appendingPathComponent(id)
            let bundle = ContainerClient.Bundle(path: path)
            return [
                try FileHandle(forReadingFrom: bundle.containerLog),
                try FileHandle(forReadingFrom: bundle.bootlog),
            ]
        } catch {
            throw ContainerizationError(
                .internalError,
                message: "failed to open container logs: \(error)"
            )
        }
    }

    /// Delete a container and its resources.
    public func delete(id: String, force: Bool) async throws {
        self.log.debug("\(#function)")
        let item = try self._get(id: id)
        switch item.snapshot.status {
        case .running:
            if !force {
                throw ContainerizationError(
                    .invalidState,
                    message: "container \(id) is \(item.snapshot.status) and can not be deleted"
                )
            }
            let autoRemove = try getContainerCreationOptions(id: id).autoRemove
            let opts = ContainerStopOptions(
                timeoutInSeconds: 5,
                signal: SIGKILL
            )
            let client = try item.getClient()
            try await client.stop(options: opts)
            if autoRemove {
                return
            }
            try self._cleanup(id: id)
        case .stopping:
            throw ContainerizationError(
                .invalidState,
                message: "container \(id) is \(item.snapshot.status) and can not be deleted"
            )
        default:
            try self._cleanup(id: id)
        }
    }

    private static func fullLaunchdServiceLabel(runtimeName: String, instanceId: String) -> String {
        "\(Self.launchdDomainString)/\(Self.machServicePrefix).\(runtimeName).\(instanceId)"
    }

    private func _cleanup(id: String) throws {
        self.log.debug("\(#function)")

        let path = self.containerRoot.appendingPathComponent(id)
        let bundle = ContainerClient.Bundle(path: path)
        let config = try bundle.configuration

        let label = Self.fullLaunchdServiceLabel(
            runtimeName: config.runtimeHandler,
            instanceId: id
        )
        try ServiceManager.deregister(fullServiceLabel: label)
        try bundle.delete()
        self.containers.removeValue(forKey: id)
    }

    private func cleanup(id: String, context: AsyncLock.Context) throws {
        try self._cleanup(id: id)
    }

    private func getContainerCreationOptions(id: String) throws -> ContainerCreateOptions {
        let path = self.containerRoot.appendingPathComponent(id)
        let bundle = ContainerClient.Bundle(path: path)
        let options: ContainerCreateOptions = try bundle.load(filename: "options.json")
        return options
    }

    private func containerProcessExitHandler(_ id: String, _ exitCode: Int32, context: AsyncLock.Context) async {
        self.log.info("Handling container \(id) exit. Code \(exitCode)")
        do {
            var item = try self.get(id: id, context: context)
            item.snapshot.status = .stopped
            item.snapshot.networks = []
            await self.setContainer(id, item, context: context)

            let options = try getContainerCreationOptions(id: id)
            if options.autoRemove {
                try self.cleanup(id: id, context: context)
            }
        } catch {
            self.log.error(
                "Failed to handle container exit",
                metadata: [
                    "id": .string(id),
                    "error": .string(String(describing: error)),
                ])
        }
    }

    private func getInitBlock(for platform: Platform) async throws -> Filesystem {
        let initImage = try await ClientImage.fetch(reference: ClientImage.initImageRef, platform: platform)
        var fs = try await initImage.getCreateSnapshot(platform: platform)
        fs.options = ["ro"]
        return fs
    }

    private static func registerService(
        plugin: Plugin,
        loader: PluginLoader,
        configuration: ContainerConfiguration,
        path: URL
    ) throws {
        let args = [
            "--root", path.path,
            "--uuid", configuration.id,
            "--debug",
        ]
        try loader.registerWithLaunchd(
            plugin: plugin,
            pluginStateRoot: path,
            args: args,
            instanceId: configuration.id
        )
    }

    private func get(id: String, context: AsyncLock.Context) throws -> ContainerState {
        try self._get(id: id)
    }

    private func _get(id: String) throws -> ContainerState {
        let item = self.containers[id]
        guard let item else {
            throw ContainerizationError(
                .notFound,
                message: "container with ID \(id) not found"
            )
        }
        return item
    }
}

extension XPCMessage {
    func signal() throws -> Int64 {
        self.int64(key: .signal)
    }

    func stopOptions() throws -> ContainerStopOptions {
        guard let data = self.dataNoCopy(key: .stopOptions) else {
            throw ContainerizationError(.invalidArgument, message: "empty StopOptions")
        }
        return try JSONDecoder().decode(ContainerStopOptions.self, from: data)
    }

    func setState(_ state: SandboxSnapshot) throws {
        let data = try JSONEncoder().encode(state)
        self.set(key: .snapshot, value: data)
    }

    func stdio() -> [FileHandle?] {
        var handles = [FileHandle?](repeating: nil, count: 3)
        if let stdin = self.fileHandle(key: .stdin) {
            handles[0] = stdin
        }
        if let stdout = self.fileHandle(key: .stdout) {
            handles[1] = stdout
        }
        if let stderr = self.fileHandle(key: .stderr) {
            handles[2] = stderr
        }
        return handles
    }

    func setFileHandle(_ handle: FileHandle) {
        self.set(key: .fd, value: handle)
    }

    func processConfig() throws -> ProcessConfiguration {
        guard let data = self.dataNoCopy(key: .processConfig) else {
            throw ContainerizationError(.invalidArgument, message: "empty process configuration")
        }
        return try JSONDecoder().decode(ProcessConfiguration.self, from: data)
    }
}
