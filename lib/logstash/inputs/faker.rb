# encoding: utf-8
require "logstash-core"
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util"
require "logstash/util/decorators"
require "json"
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

  config :primary_key, :validate => :hash, :default => {}

  config :foreign_keys, :validate => :array, :default => []

  config :foreign_key_field, :validate => :string, :default => nil

  config :add_primary_key_to_events, :validate => :boolean, :default => false

  config :add_foreign_keys_to_events, :validate => :boolean, :default => false

  config :multiply_field, :validate => :hash, :default => {}

  config :multiply_splitable_field, :validate => :hash, :default => {}

  # Similar to LogStash::Inputs::Base.add_field define a hash
  # where the value is a Faker module and method call
  # ex: Name.first_name
  config :add_faker_field, :validate => :hash, :default => {}

  # Add a splitable field. Currently only supports defining a
  # single field that will be used for generating sub-events
  config :splitable_field, :validate => :string, :default => nil

  # Add static values to the splitable field's data structure
  # See add_field for structure definition
  config :add_splitable_field, :validate => :hash, :default => {}

  # Add Faker fields to the splitable object mappings
  # These expect the same format of add_faker_field
  config :add_splitable_faker_field, :validate => :hash, :default => {}

  # define the number of entries into the array of the splitable field
  # When set to 0 a random number will be used within 1-100 as the
  # splitable field count
  config :splitable_field_count, :validate => :number, default: 0

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
    set_primary_key   if @add_primary_key_to_events
    set_foreign_keys  if @add_foreign_keys_to_events
    while !stop? && (@count <= 0 || number < @count)
      event = LogStash::Event.new({})
      add_faker_fields(event)
      if @splitable_field
        add_splitable_fields(event)
      end
      if @add_primary_key_to_events
        LogStash::Util::Decorators.add_fields(@primary_key, event,"inputs/#{self.class.name}")
      end
      decorate(event)
      event.set("host", @host)
      queue << event
      number += 1
    end #end loop
  end # end run

  protected
  def generate_multiplied_values(value, count, is_faker=false)
    num = (count == 0 ? rand(1000) : count)
    if is_faker
      return num.times.map{ Faker.class_eval(value) }
    else
      return num.times.map{ value }
    end
  end

  protected
  def add_faker_fields(event)
    new_fields = {}
    @add_faker_field.each do |field, faker_string|
      if @multiply_field[field]
        event.remove(field) if @overwrite_fields
        new_fields[field] = generate_multiplied_values(faker_string, @multiply_field[field], true)
      else
        event.remove(field) if @overwrite_fields
        new_fields[field] = Faker.class_eval(faker_string)
      end
    end
    LogStash::Util::Decorators.add_fields(new_fields, event,"inputs/#{self.class.name}")
  end

  protected
  def add_splitable_fields(event)
    event.remove(@splitable_field)
    splitable_events = []
    used_ids = []
    if @splitable_field_count <= 0
      @splitable_field_count = rand(100)
    end
    @splitable_field_count.times do
      new_event = LogStash::Event.new()
      @add_splitable_field.each do |field, value|
        if @multiply_splitable_field[field]
          new_event.set(field, generate_multiplied_values(faker_string, @multiply_splitable_field[field], false))
        else
          new_event.set(field, event.sprintf(value))
        end
      end
      @add_splitable_faker_field.each do |field, faker_string|
        if @multiply_splitable_field[field]
          new_event.set(field, generate_multiplied_values(faker_string, @multiply_splitable_field[field], true))
        else
          new_event.set(field, event.sprintf(Faker.class_eval(faker_string)))
        end
      end
      if @add_foreign_keys_to_events
        key = (@foreign_keys - used_ids).sample
        if key.nil?
          used_ids = []
          key = (@foreign_keys - used_ids).sample
        else
          new_event.set(@foreign_key_field, key)
        end
        used_ids.push(key)
      end
      new_event.remove("@timestamp")
      new_event.remove("@version")
      splitable_events << new_event.to_hash
    end
    event.set(@splitable_field, splitable_events)
  end

  protected
  def set_primary_key
    if @add_primary_key_to_events && @primary_key.empty?
      @primary_key = { "[id]" => Faker::Number.number(10).to_s }
    end
  end

  protected
  def set_foreign_keys
    if @add_foreign_keys_to_events && @foreign_keys.empty?
      @foreign_key_field = "[foreign_id]" if @foreign_key_field.to_s.empty?
      if @splitable_field_count <= 0
        if @count <= 0
          generate_foreign_keys(@count)
        else
          generate_foreign_keys(1000)
        end
      else
        generate_foreign_keys(@splitable_field_count)
      end
    end
  end

  protected
  def generate_foreign_keys(key_count)
    @foreign_keys = []
    while @foreign_keys.size < key_count
      @foreign_keys.push(Faker::Number.number(10).to_s)
      @foreign_keys.uniq!
    end
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