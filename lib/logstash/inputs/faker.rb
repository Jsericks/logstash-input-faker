# encoding: utf-8
# require "logstash-core"
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/decorators"
require "faker"
require "socket" # for Socket.gethostname

I18n.reload!

class LogStash::Inputs::Faker < LogStash::Inputs::Base
  config_name "faker"

  default :codec, "plain"

  config :add_faker_field, :validate => :hash, :default => {}

  config :overwrite_fields, :validate => :boolean, :default => false

  config :count, :validate => :number, :default => 0

  public
  def register
    @host = Socket.gethostname
    @count = Array(@count).first
  end

  def run(queue)
    number = 0

    while !stop? && (@count <= 0 || number < @count)
      event = LogStash::Event.new({})
      add_faker_fields(event)
      decorate(event)
      event.set("host", @host)
      queue << event
      number += 1
    end
  end

  protected
  def add_faker_fields(event)
    new_fields = {}
    @add_faker_field.each do |field, faker_string|
      event.remove(field) if @overwrite_fields
      new_fields[field] = Faker.class_eval(faker_string)
    end
    LogStash::Util::Decorators.add_fields(new_fields,event,"inputs/#{self.class.name}")
  end

  public
  def close
    if @codec.respond_to?(:flush)
      @codec.flush do |event|
        decorate(event)
        event.set("host", @host)
        queue << event
      end
    end
  end
end