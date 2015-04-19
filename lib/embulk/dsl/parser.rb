module Embulk

  class DSLParser
    def self.parse(config_string)
      SimpleDSLElement.new.instance_eval(config_string).to_config_object
    end
  end

  class SimpleDSLElement

    def initialize
      @element = {}
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
=end
