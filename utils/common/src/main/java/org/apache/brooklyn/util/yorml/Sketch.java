package org.apache.brooklyn.util.yorml;

public class Sketch {

/*

// MOTIVATION

We want a JSON/YAML schema which allows us to do bi-directional serialization to Java with docgen.
That is:
* It is easy for a user to write the YAML which generates the objects they care about
* It is easy for a user to read the YAML generated from data objects
* The syntax of the YAML can be documented automatically from the schema
* JSON can also be read or written (we restrict to the subset of YAML which is isomorphic to JSON)

The focus on ease-of-reading and ease-of-writing differentiates this from other JSON/YAML
serialization processes.  For instance we want to be able to support the following polymorphic
expressions:

shapes:
- type: square     # standard, explicit type and fields, but hard to read
  size: 12
  color: red
- square:          # type implied by key
    size: 12
    color: red
- square: 12       # value is taken as a default key in a map
- red_square   # string on its own can be interpreted in many ways but often it's the type
# and optionally (deferred)
- red square: { size: 12 }   # multi-word string could be parsed in many ways (a la css border)

Because in most contexts we have some sense of what we are expecting, we can get very efficient
readable representations.

Of course you shouldn't use all of these to express the same type; but depending on the subject 
matter some syntaxes may be more natural than others.  Consider allowing writing:

  effectors:   # field in parent, expects list of types 'Effector' 
    say_hi:    # map converts to list treating key as name of effector, expecting type 'Effector' as value
      type: ssh     # type alias 'ssh' when type 'Effector` is needed matches SshEffector type
      parameters:   # field in SshEffector, of type Parameter
      - name        # string given when Parameter expected means it's the parameter name
      - name: hello_word                  # map of type, becomes a Parameter populating fields
        description: how to say hello
        default: hello 
      command: |                          # and now the command, which SshEffector expects
        echo ${hello_word} ${name:-world}

The important thing here is not using all of them at the same time (as we did for shape), 
but being *able* to support an author picking the subset that is right for a given situation, 
in a way that they can be parsed, they can be generated, and the expected/supported syntax 
can be documented automatically.
   

// INRODUCTORY EXAMPLES

* defining types

When defining a type, an `id` (how it is known) and an instantiable `type` (parent type) must be supplied.
These are kept in a type registry and can be used when defining other types or instances.

# should be supplied
- id: shape
  # no-arg constructor
  type: java:org.acme.Shape  # where `class Shape { String name; String color; }`

You can also define types with default field values set, and of course you can refer to types 
that have been defined:

- id: red-square
  type: shape
  fields:
    # any fields here read/written by direct access by default, or fail if not matched
    name: square
    color: red

There are many syntaxes for defining instances, described below.  Most of these can
be used when defining new types.

* defining instances

You define an instance by referencing a type, and optionally specifying fields:

  type: red-square

Or

- type: shape
  fields:
    name: square
    color: red


* overwriting fields

You could do this:

- id: pink-square
  type: red-square
  fields:
    # map of fields is merged with that of parent
    color: pink

Although this would be more sensible:

- id: square
  type: shape
  fields:
    name: square
- id: pink-square
  type: square
  fields:
    color: pink


* allowing fields at root 
  (note: automatically the case in some situations)

- id: ez-square
  type: square
  serialization:
  - type: explicit-field
    field-name: color
  - type: no-others

then (instance)

- type: ez-square
  color: blue

Serialization takes a list of serializer types.  These are applied in order, both for serialization 
and deserialization, and re-run from the beginning if any are applied.

`explicit-field` says to look at the root as well as in the 'fields' block.  It has one required
parameter, field-name, and several optional ones:

  - type: explicit-field
    field-name: color
    key-name: color      # this is used in yaml
    aliases: [ colour ]  # accepted in yaml as a synonym for key-name; `alias` also accepted
    field-type: string   # inferred from java field, but you can constrain further to yaml types
    constraint: required # currently just supports 'required', or blank for none, but reserved for future use
    description: The color of the shape   # text (markdown) 
    serialization:       # optional additional serialization instructions
    - if-string:         # (defined below)
        set-key: field-name

`no-others` says that any unrecognised fields in YAML will force an error prior to the default
deserialization steps (which attempt to write named config and then fields directly, before failing),
and on serialization it will ignore any unnamed fields.

As a convenience if an entry in the list is a string S, the entry is taken as 
`{ type: explicit-field, field-name: S }`.

Thus the following would also be allowed:

  serialization:
  - color
  - type: no-others


// ADVANCED

At the heart of this YAML serialization is the idea of heavily overloading to permit the most
natural way of writing in different situations. We go a bit overboard in 'serialization' to illustrate
below the different strategies. (Feel free to ignore, if you're comfortable with the simple examples.)
If the `serialization` field (which expects a list) is given a map, each <K,V> pair in that map is 
interpreted as follows:
* if V is a map then K is set in that map as 'field-name' (error if field-name is already set)
* if V is not a map then a map is created as { field-name: K, type: V }
Thus you could also write:

  serialization:
    color: { alias: colour, description: "The color of the shape", constraint: required } 

(Note that some serialization types, such as 'no-others', cannot be expressed in this way,
because `field-name` is not supported on that type. This syntax is intended for the common 
case when all fields are settable and we are defining top-level fields.)

Finally if the serialization is given a list, and any entry in the list is a map which
does not define a type, the following rules apply:
* If the entry is a map of size larger than one, the type defaults to explicit-field.  
* If the entry is a map of size one the key is taken as the type and merged with the value
  if the value is a map (or it can be interpreted as such with an if-string on the type)
Thus we can write:

  serialization:
  - field-name: color
    alias: colour
  - no-others:

Note: this has some surprising side-effects in occasional edge cases; consider:

  # BAD: this would try to load a serialization type 'field-name' 
  serialization:
  - field-name: color
  # GOOD options
  serialization:
  - color
  # or
  serialization:
    color:
  # or
  serialization:
  - field-name: color
    alias: colour  

  # BAD: this would define a field `explicitField`, then fail because that field-name is in use
  serialization:
    explicit-field: { field-name: color }
  # GOOD options (in addition to those in previous section, but assuming you wanted to say the type explciitly)
  serialization:
  - explicit-field: { field-name: color }
  # or 
  - explicit-field: color

It does the right thing in most cases, and it serves to illustrate the flexibility of this
approach. In most cases it's probably a bad idea to do this much overloading!  However the
descriptions here will normally be taken from java annotations and not written by hand,
so emphasis is on making it easy-to-read (which overloading does nicely) rather than 
easy-to-write.

Of course if you have any doubt, simply use the long-winded syntax and avoid any convenience syntax:

  serialization:
  - type: explicit-field
    field-name: color
    alias: colour


// OTHER BEHAVIORS

* name mangling pattern, default conversion for fields:
  wherever pattern is lower-upper-lower (java) <-> lower-dash-lower-lower (yaml)

  fields:
    # corresponds to field shapeColor 
    shape-color: red

* primitive types

All Java primitive types are known, with their boxed and unboxed names,
and the key `value` can be used to set a value. This is normally not necessary
as where a primitive is expected routines will attempt coercion, but in some
cases it is desired.  So for instance a red square could be defined as:

- type: shape
  name:
    type: string
    value: red

* config/data keys

Some java types define static ConfigKey fields and a `configure(key, value)` or `configure(ConfigBag)`
method. These are detected and applied as one of the default strategies (below).


* accepting lists with generics

Where the java object is a list, this can correspond to YAML in many ways.
New serializations we introduce include `convert-map-to-map-list` (which allows
a map value to be supplied), `apply-defaults-in-list` (which ensures a set of keys
are present in every entry, using default values wherever the key is absent),
`convert-single-key-maps-in-list` (which gives special behaviour if the list consists
entirely of single-key-maps, useful where a map would normally be supplied but there
might be key collisions), `if-string-in-list` (which applies `if-string` to every
element in the list), and `convert-map-to-singleton-list` (which puts a map into
a list).

If no special list serialization is supplied for when expecting a type of `list<x>`,
the YAML must be a list and the serialization rules for `x` are then applied.  If no
generic type is available for a list and no serialization is specified, an explicit
type is required on all entries.

Serializations that apply to lists or map entries are applied to each entry, and if
any apply the serialization is then continued from the beginning.

As a complex example, the `serialization` list we described above has the following formal
schema:

- field-name: serialization
  field-type: list<serialization>
  serialization:
  
  # transforms `- color` to `- { explicit-field: color }` which will be interpreted again
  - type: if-string-in-list
    set-key: explicit-field
    
  # alternative implementation of above (more explicit, not relying on `apply-defaults-in-list`)
  # transforms `- color` to `- { type: explicit-field, field-name: color }`
  - type: if-string-in-list
    set-key: field-name
    default:
      type: explicit-field

  # describes how a yaml map can correspond to a list
  # in this example `k: { type: x }` in a map (not a list)
  # becomes an entry `{ field-name: k, type: x}` in a list
  # (and same for shorthand `k: x`; however if just `k` is supplied it
  # takes a default type `explicit-field`)
  - type: convert-map-to-map-list
      key-for-key: field-name
      key-for-string-value: type   # note, only applies if x non-blank
      default:
        type: explicit-field       # note: needed to prevent collision with `convert-single-key-in-list` 

  # if yaml is a list containing all maps swith a single key, treat the key specially
  # transforms `- x: k` or `- x: { field-name: k }` to `- { type: x, field-name: k }`
  # (use this one with care as it can be confusing, but useful where type is the only thing
  # always required! typically only use in conjunction with `if-string-in-list` where `set-key: type`.)
  - type: convert-single-key-maps-in-list
    key-for-key: type              # NB fails if this key is present in the value which is a map
    key-for-string-value: field-name
  
  # applies any listed unset "default keys" to the given default values,
  # for every map entry in a list
  # here this essentially makes `explicit-field` the default type
  - type: apply-defaults-in-list
    default:
      type: explicit-field


* accepting maps with generics

In some cases the underlying type will be a java Map.  The lowest level way of representing a map is
as a list of maps specifying the key and value of each entry, as follows:

- key:
    type: string
    value: key1
  value:
    type: red-square
- key:
    type: string
    value: key2
  value: a string

You can also use a more concise map syntax if keys are strings:

  key1: { type: red-square }
  key2: "a string"

If we have information about the generic types -- supplied e.g. with a type of `map<K,V>` --
then coercion will be applied in either of the above syntaxes.


* where the accepted type is unknown

In some instances an expected type may be explicitly `java.lang.Object`, or it may be
unknown (eg due to generics).  In these cases if no serialization rules are specified,
we take lists as lists, we take maps as objects if a `type` is defined, we take
primitives when used as keys in a map as those primitives, and we take other primitives
as *types*.  This last is to prevent errors.  It is usually recommended to ensure that 
either an expected type will be known or serialization rules are supplied (or both).   


* default serialization

It is possible to set some serializations to be defaults run before or after a supplied list.
The default is to run the following after (but this is suppressed if `no-others` is supplied,
in which case you may want to use some before the `no-others` directive):

  * `expected-type-serializers` runs through the serializers defined on the expected type
    (so e.g. a shape might define a default size, then a reference to shape would always have that)
 * `instantiate-type` on read, converts a map to the declared type (this can be used explicitly
    to give information on which fields should be used as parameters in constructors,
    or possibly to reference a static factory method)
  * `all-config` reads/writes all declared config keys if there is a `configure(...)` method
  * `all-matching-fields` reads any key corresponding to a field into an object, or writes all remaining
    non-transient fields
  * `fields-in-fields-map` applies all the keys in a `fields` block as fields in the object

// TODO

* type overloading, if string, if number, if map, if list...  inferring type, or setting diff fields
* super-types and abstract types (underlying java of `supertypes` must be assignable from underying java of `type`)
* merging ... deep? field-based?
* setting in java class with annotation
* if-list, if-map, if-key-present, etc
* fields fetched by getters, written by setters
* include/exclude if null/empty/default


// IMPLEMENTATION SKETCH


## Yorma.java

typeRegistry
converter

read
  yamlObject
  yormaContext(jsonPath,expectedType)
  yamlContext(origin,offset,length)
returns object of type expectedType
makes shallow copy of the object, then goes through serializers modifying it or creating/setting result, 
until result is done 

write
  object
  yormaContext(jsonPath,expectedType)
returns jsonable object (map, list, primitive)   

document(type)
generates human-readable schema for a type

## Serializer.java

read/write
    as above, but also taking YormaConversion and with a ConfigBag blackboard




  
// TO ALLOW

- id: print-all
  type: initdish-effector
  steps:
    00-provision: provision
    10-install:
      type: bash
      contents: |
        curl blah
        tar blah
    20-run:
      type: invoke-effector
      effector: launch
      parameters:
        ...

- type: entity
  fields:
  - key: effectors
    yamlType: list
    yamlGenericType: effector
    serializer:
    - type: write-list-field
      field: effectors
- type: effector


 */
    
}
