# encoding: utf-8
# require "logstash-core"
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/decorators"
require "faker"
require "socket" # for Socket.gethostname

I18n.reload!

# [NOTE]
# ===================================================================
# The values put into the add_faker_field params must be available
# modules and method calls from the Faker gem library seen at
# https://github.com/stympy/faker/
# ===================================================================
# Example: 
# [source, ruby]
#     input {
#       faker {
#         count => 1
#         add_faker_field => {
#           "[name][last_name]" => "Name.last_name"
#           "[name][first_name]" => "Name.first_name"
#           "[address][city]" => "Address.city"
#           "[address][address1]" => "Address.street_address"
#         }
#         overwrite_fields => true
#       }
#     }

class LogStash::Inputs::Faker < LogStash::Inputs::Base
  config_name "faker"

  default :codec, "plain"

  config :add_faker_field, :validate => :hash, :default => {}

  # Will overwrite fields that are in the event prior to add_faker_field
  # being invoked ( usually fields created using add_field ) if true
  # when false this will insert the new faker values into an array with
  # the previously defined event values
  config :overwrite_fields, :validate => :boolean, :default => false

  # The number of events to generate
  # When not explicitly defined plugin will
  # generate events until stopped
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
    end #end loop
  end # end run

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