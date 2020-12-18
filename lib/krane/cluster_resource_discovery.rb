# frozen_string_literal: true

module Krane
  class ClusterResourceDiscovery
    delegate :namespace, :context, :logger, to: :@task_config

    def initialize(task_config:, namespace_tags: [])
      @task_config = task_config
      @namespace_tags = namespace_tags
    end

    def crds
      @crds ||= fetch_crds.map do |cr_def|
        CustomResourceDefinition.new(namespace: namespace, context: context, logger: logger,
          definition: cr_def, statsd_tags: @namespace_tags)
      end
    end

    def prunable_resources(namespaced:)
      black_list = %w(Namespace Node ControllerRevision NodeProxyOptions)
      fetch_resources(namespaced: namespaced).uniq { |r| r["kind"] }.map do |resource|
        next unless resource["verbs"].one? { |v| v == "delete" }
        next if black_list.include?(resource["kind"])
        version = resource["version"]
        [resource["apigroup"], version, resource["kind"]].compact.join("/")
      end.compact
    end

    def fetch_resources(namespaced: false)
      api_paths.flat_map do |path|
        resources = fetch_api_path(path)["resources"] || []
        resources.map { |r| resource_hash(path, namespaced, r) }.compact
      end
    end

    private

    def api_paths
      raw_json, err, st = kubectl.run("get", "--raw", "/", attempts: 5, use_namespace: false)
      paths = if st.success?
        JSON.parse(raw_json)["paths"]
      else
        raise FatalKubeAPIError, "Error retrieving raw path /: #{err}"
      end
      paths.select { |path| path.start_with?("/api") }
    end

    def fetch_api_path(path)
      raw_json, err, st = kubectl.run("get", "--raw", path, attempts: 5, use_namespace: false)
      if st.success?
        JSON.parse(raw_json)
      else
        raise FatalKubeAPIError, "Error retrieving api path: #{err}"
      end
    end

    def resource_hash(path, namespaced, blob)
      return unless blob["namespaced"] == namespaced
      return unless blob["verbs"] && blob["kind"]

      path_regex = /(\/apis?\/)(?<group>[^\/]*)\/?(?<version>v.+)/
      match = path.match(path_regex)
      group = match[:group]
      version = match[:version]
      {
        "verbs" => blob["verbs"],
        "kind" => blob["kind"],
        "apigroup" => group,
        "version" => version,
      }
    end

    def fetch_crds
      raw_json, err, st = kubectl.run("get", "CustomResourceDefinition", output: "json", attempts: 5,
        use_namespace: false)
      if st.success?
        JSON.parse(raw_json)["items"]
      else
        raise FatalKubeAPIError, "Error retrieving CustomResourceDefinition: #{err}"
      end
    end

    def kubectl
      @kubectl ||= Kubectl.new(task_config: @task_config, log_failure_by_default: true)
    end
  end
end
