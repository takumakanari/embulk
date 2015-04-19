module Embulk

  class DSLParser
    def self.parse(config_string)
      c = SimpleDSLElement.new.instance_eval(config_string)
      DSLConfig.new(c)
    end
  end

  class DSLConfig
  
    def initialize(root_config)
      @root_config = root_config
    end

    def root_element
      @root_config.to_config_object
    end

    def exec_event(name, *args)
      key = :"on_#{name}"
      callbacks = @root_config.callbacks
      unless callbacks.has_key?(key)
        raise "Unknown event on_#{name}"
      end
      callbacks[key].call(*args) unless callbacks[key].nil?
    end
  end

  class SimpleDSLElement
    attr_reader :callbacks

    def initialize
      @element = {}
      @callbacks = {
        :on_start => nil,
        :on_complete => nil
      }
    end

    def input(*args, &block)
      arg_required(args)
      name = "in"
      add_element(name, &block)
      merge_extra_properties(name, {:type => args.first})
      self
    end

    def output(*args, &block)
      arg_required(args)
      name = "out"
      add_element(name, &block)
      merge_extra_properties(name, {:type => args.first})
      self
    end

    def on_start(&block)
      @callbacks[:on_start] = block
      self
    end

    def on_complete(&block)
      @callbacks[:on_complete] = block
      self
    end

    # TODO def exec(*args, &block); end

    def method_missing(name, *args, &block)
      raise ArgumentError, "Embulk DSL Syntax Error: only one argument allowed" if args.size > 1
      value = args.first
      if block
        add_element(name, &block)
        merge_extra_properties(name, type: value) unless value.nil?
      elsif value.instance_of?(Array) || value.instance_of?(Hash)
        @element[name.to_s] = value # TODO do we have to parse also nested block?
      else
        @element[name.to_s] = value.to_s
      end
      self
    end

    def to_config_object; @element end

    private

    def add_element(name, &block)
      raise ArgumentError, "#{name} block must be specified" if block.nil?
      element = self.class.new
      element.instance_exec(&block)
      @element[name] = element.to_config_object
      self
    end

    def merge_extra_properties(name, props)
      @element[name].merge!(props)
    end

    def arg_required(args)
      raise ArgumentError, "#{name} block requires one argument" if args.nil? || args.size != 1
      true
    end

  end
end

=begin
Ruby-style DSL configuration is as below:

on_start {
  # callback on start task
  puts "start!"
}

input("file") {

  path_prefix "example/csv/sample_"

  decoders [
    {type: "gzip"}
  ]

  parser("csv") {
    charset "UTF-8"
    newline "CRLF"
    skip_header_lines 1
    columns [
      {name: "id", type: "long"},
      {name: "account", type: "long"},
      {name: "time", type: "timestamp", format: "%Y-%m-%d %H:%M:%S"},
      {name: "purchase", type: "timestamp", format: "%Y%m%d"},
      {name: "comment", type: "string"}
    ]
  }

}

output("stdout") {
}

on_complete {|config_diff|
  # callback on complete task
  puts "diff: #{config_diff}"
}
=end
