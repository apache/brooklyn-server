%
% Licensed to the Apache Software Foundation (ASF) under one
% or more contributor license agreements.  See the NOTICE file
% distributed with this work for additional information
% regarding copyright ownership.  The ASF licenses this file
% to you under the Apache License, Version 2.0 (the
% "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.
%

# YOML: The YAML Object Mapping Language

## Motivation

We want a JSON/YAML schema which allows us to do bi-directional serialization to Java with docgen.
That is:
* It is easy for a user to write the YAML which generates the objects they care about
* It is easy for a user to read the YAML generated from data objects
* The syntax of the YAML can be documented automatically from the schema (including code-point-completion)
* JSON can also be read or written (we restrict to the subset of YAML which is isomorphic to JSON)

The focus on ease-of-reading and ease-of-writing differentiates this from other JSON/YAML
serialization processes.  For instance we want to be able to support the following polymorphic
expressions:

```
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
```

Because in most contexts we have some sense of what we are expecting, we can get very efficient
readable representations.

Of course you shouldn't use all of these to express the same type; but depending on the subject 
matter some syntaxes may be more natural than others.  Consider allowing writing:

```
  effectors:   # field in parent, expects list of types 'Effector' 
    say_hi:    # map converts to list treating key as name of effector, expecting type 'Effector' as value
      type: ssh     # type alias 'ssh' when type 'Effector` is needed matches SshEffector type
      parameters:   # field in SshEffector, of type Parameter
      - person      # string given when Parameter expected means it's the parameter name
      - name: hello_word                  # map of type, becomes a Parameter populating fields
        description: how to say hello
        default: hello 
      command: |                          # and now the command, which SshEffector expects
        echo ${hello_word} ${person:-world}
```

The important thing here is not using all of them at the same time (as we did for shape), 
but being *able* to support an author picking the subset that is right for a given situation, 
in a way that they can be parsed, they can be generated, and the expected/supported syntax 
can be documented automatically.
   

## Introductory Examples


### Defining types and instances

You define a type by giving an `id` (how it is known) and an instance definition specifying the parent `type`. 
These are kept in a type registry and can be used when defining other types or instances.
A "type definition" looks like:

```
- id: shape
  definition:
    type: java:org.acme.Shape  # where `class Shape { String name; String color; }`
```

The `java:` prefix is an optional shorthand to allow a Java type to be accessed.
For now this assumes a no-arg constructor.

You can then specify an instance to be created by giving an "instance definition",
referring to a defined `type` and optionally `fields`:

```
- type: shape
  fields:  # optionally
    name: square
    color: red
``` 

Type definitions can also refer to types already defined types and can give an instance definition:

```
- id: red-square
  definition:
    type: shape
    fields:
      # any fields here read/written by direct access by default, or fail if not matched
      name: square
      color: red
```

The heart of YOML is the extensible support for other syntaxes available, described below.
These lead to succinct and easy-to-use definitions, both for people to write and for people to read
even when machine-generated.  The approach also supports documentation and code-point completion.


### Instance definitions

You define an instance to be created by referencing a type in the registry, and optionally specifying fields:

    type: red-square

Or

    type: shape
    fields:
      name: square
      color: red


### Type definitions

You define a new type in the registry by giving an `id` and the instance `definition`:

    id: red-square
    definition:
      type: shape
      fields:
        name: square
        color: red

Where you just want to define a Java class, a shorthand permits providing `type` instead of the `definition`: 

    id: shape
    type: java:org.acme.Shape


### Overwriting fields

Fields can be overwritten, e.g. to get a pink square:

    type: red-square
    fields:
      # map of fields is merged with that of parent
      color: pink

You can do this in type definitions, so you could do this:

```
- id: pink-square
  type: red-square
  definition:
    fields:
      # map of fields is merged with that of parent
      color: pink
```

Although this would be more sensible:

```
- id: square
  definition:
    type: shape
    fields:
      name: square
- id: pink-square
  definition:
    type: square
    fields:
      color: pink
```


### Allowing fields at root

Type definitions also support specifying additional "serializers", the workers which provide
alternate syntaxes. One common one is the `top-level-field` serializer, allowing fields at
the root of an instance definition.  With this type defined:
 
- id: ez-square
  type: square
  serialization:
  - type: top-level-field
    field-name: color

You could skip the `fields` item altogether and write:

```
type: ez-square
color: pink
```

These are inherited, so we'd probably prefer to have these type definitions:

```
- id: shape
  definition:
    type: java:org.acme.Shape  # where `class Shape { String name; String color; }`
  serialization:
  - type: top-level-field
    field-name: name
  - type: top-level-field
    field-name: color
- id: square
  definition:
    type: shape
    name: square
```

## Intermission: On serializers and implementation (can skip)
 
Serialization takes a list of serializer types.  These are applied in order, both for serialization 
and deserialization, and re-run from the beginning if any are applied.

`top-level-field` says to look at the root as well as in the 'fields' block.  It has one required
parameter, field-name, and several optional ones, so a sample usage might look like:

```
  - type: top-level-field
    field-name: color
    key-name: color           # this is used in yaml
    aliases: [ colour ]       # things to accept in yaml as synonyms for key-name; `alias` also accepted
    aliases-strict: false     # if true, means only exact matches on key-name and aliases are accepted, otherwise a set of mangles are applied
    aliases-inherited: true   # if false, means only take aliases from the first top-level-field serializer for this field-name, otherwise any can be used 
    # TODO items below here are still WIP/planned
    field-type: string        # inferred from java field, but you can constrain further to yaml types
    constraint: required      # currently just supports 'required' (and 'null' not allowed) or blank for none (default), but reserved for future use
    description: The color of the shape   # text (markdown) 
    serialization:            # optional additional serialization instructions for this field
    - convert-from-primitive:   # (defined below)
        key: field-name
```


## Further Behaviours

### Name Mangling and Aliases

We apply a default conversion for fields: 
wherever pattern is lower-upper-lower (java) <-> lower-dash-lower-lower (yaml).
These are handled as a default set of aliases.

    fields:
      # corresponds to field shapeColor 
      shape-color: red


### Primitive types

All Java primitive types are known, with their boxed and unboxed names,
along with `string`.  The key `value` can be used to set a value for these.
It's not normally necessary to do this because the parser can usually detect
these types and coercion will be applied wherever one is expected;
it's only needed if the value needs coercing and the target type isn't implicit.
For instance a red square with size 8 could be defined as:

```
- type: shape
  color:
    type: string
    value: red
  size:
    type: int
    value: 8
```

Or of course the more concise:

```
- type: shape
  color: red
  size: 8
```


### Config/data keys

Some java types define static ConfigKey fields and a `configure(key, value)` or `configure(ConfigBag)`
method. These are detected and applied as one of the default strategies (below).


### Accepting lists, including generics

Where the java object is a list, this can correspond to YAML in many ways.
The simplest is where the YAML is a list, in which case each item is parsed,
including serializers for the generic type of the list if available.

Because lists are common in object representation, and because the context
might have additional knowledge about how to intepret them, a list field
(or any context where a list is expected) can declare additional serializers.
This can result in much nicer YOML representations.

Serializers available for this include:

* `convert-singleton-map` which can specify how to convert a map to a list,
  by taking each <K,V> pair and treating it as a list of f(<K,V>) where
  f maps the K and V into a new map, e.g. embedding K with a default key 
* `default-map-values` which ensures a set of keys are present in every entry, 
  using default values wherever the key is absent
* `convert-singleton-maps-in-list` which gives special behaviour if a list consists
  entirely of single-key-maps, useful where a user might want to supply a concise map 
  syntax but that map would have several keys the same
* `convert-from-primitive` which converts a primitive to a map

If no special list serialization is supplied for when expecting a type of `list<x>`,
the YAML must be a list and the serialization rules for `x` are then applied.  If no
generic type is available for a list and no serialization is specified, an explicit
type is required on all entries.

Serializations that apply to lists are applied to each entry, and if any apply the 
serialization is then continued from the beginning (unless otherwise noted).


#### Complex list serializers (skip on first read!)

At the heart of this YAML serialization is the idea of heavily overloading to permit the most
natural way of writing in different situations. We go a bit overboard in some of the `serialization` 
examples to illustrate the different strategies and some of the subtleties. (Feel free to ignore 
until and unless you need to know details of complex strategies.)

As a complex example, to define serializations for shape, the basic syntax is as follows:

    serialization:
    - type: top-level-field
      field-name: color
      alias: colour
      description: "The color of the shape" 
    - type: top-level-field
      field-name: name
    - type: convert-from-primitive
      key: color

However we can also support these simplifications:
 
    serialization:
    - field-name: color
      alias: colour
    - name
    - convert-from-primitive: color

    serialization:
      name: {}
      color: { alias: colour, description: "The color of the shape", constraint: required } 
      colour: { type: convert-from-primitive }

This works because we've defined the following sets of rules for serializing serializations:

```
- field-name: serialization
  field-type: list<serializer>
  serialization:
  
  # given `- name` rewrite as `- { top-level-field: name }`, which will then be further rewritten
  - type: convert-from-primitive
    key: top-level-field
    
  # alternative implementation of above (more explicit, not relying on singleton map conversion)
  # e.g. transforms `- name` to `- { type: top-level-field, field-name: name }`
  - type: convert-from-primitive
    key: field-name
    defaults:
      type: top-level-field

  # if yaml is a list containing maps with a single key, treat the key specially
  # transforms `- x: k` or `- x: { .value: k }` to `- { type: x, .value: k }`
  # (use this one with care as it can be confusing, but useful where type is the only thing
  # always required; it's recommended (although not done in this example) to use alongside 
  # a rule `convert-from-primitive` with `key: type`.)
  - type: convert-singleton-maps-in-list
    key-for-key: type              # NB skipped if the value is a map containing this key
    # if the value is a map, they will merge
    # otherwise the value is set as `.value` for conversion later

  # describes how a yaml map can correspond to a list
  # in this example `k: { type: x }` in a map (not a list)
  # becomes an entry `{ field-name: k, type: x}` in a list
  # (and same for shorthand `k: x`; however if just `k: {}` is supplied it
  # takes a default type `top-level-field`)
  - type: convert-singleton-map
    key-for-key: .value          # as above, will be converted later
    key-for-string-value: type   # note, only applies if x non-blank
    defaults:
      type: top-level-field      # note: this is needed to prevent collision with rule above 
  
  # applies any listed unset default keys to the given default values,
  # either on a map, or if a list then for every map entry in the list;
  # here this essentially makes `top-level-field` the default type
  - type: default-map-values
    defaults:
      type: top-level-field
```

We also rely on `top-level-field` having a rule `rename-default-value: field-name`
and `convert-from-primitive` having a rule `rename-default-value: key`
to convert the `.value` key appropriately for those types.

This can have some surprising side-effects in edge cases; consider:

```
  # BAD: this would try to load a type called 'color' 
  serialization:
  - color: {}
  # GOOD options
  serialization:
  - color
  # or
  serialization:
    color: {}
  # or
  serialization:
    color: top-level-field

  # BAD: this would try to load a type called 'field-name' 
  serialization:
  - field-name: color
  # GOOD options are those in the previous block or to add another field
  serialization:
  - field-name: color
    alias: colour  

  # BAD: this ultimately takes "top-level-field" as the "field-name", giving a conflict
  serialization:
    top-level-field: { field-name: color }
  # GOOD options (in addition to those in previous section, but assuming you wanted to say the type explicitly)
  serialization:
  - top-level-field: { field-name: color }
  # or 
  - top-level-field: color
```

In most cases it's probably a bad idea to do this much overloading!
But here it does the right thing in most cases, and it serves to illustrate the flexibility of this approach.

The serializer definitions will normally be taken from java annotations and not written by hand,
so emphasis should be on making type definitions easy-to-read (which overloading does nicely), and 
instance definitions both easy-to-read and -write, rather than type definitions easy-to-write.

Of course if you have any doubt, simply use the long-winded syntax:

```
  serialization:
  - type: top-level-field
    field-name: color
```


### Accepting maps, including generics

In some cases the underlying type will be a java Map.  The lowest level way of representing a map is
as a list of maps specifying the key and value of each entry, as follows:

```
- key:
    type: string
    value: key1
  value:
    type: red-square
- key:
    type: string
    value: key2
  value: a string
```

You can also use a more concise map syntax if keys are strings:

    key1: { type: red-square }
    key2: "a string"

If we have information about the generic types -- supplied e.g. with a type of `map<K,V>` --
then coercion will be applied in either of the above syntaxes.


### Where the expected type is unknown

In some instances an expected type may be explicitly `java.lang.Object`, or it may be
unknown (eg due to generics).  In these cases if no serialization rules are specified,
we take lists as lists, we take maps as objects if a `type` is defined, we take
primitives when used as keys in a map as those primitives, and we take other primitives
as *types*.  This last is to prevent errors.  It is usually recommended to ensure that 
either an expected type will be known or serialization rules are supplied (or both).   


### Default serialization

It is possible to set some serializations to be defaults run before or after a supplied list.
This is useful if for instance you want certain different default behaviours across the board.
Note that if interfacing with the existing defaults you wil need to understand that process
in detail; see implementation notes below. 


## Behaviors which are **not** supported (yet)

* multiple references to the same object
* include/exclude if null/empty/default
* controlling deep merge behaviour (currently collisions at keys are not merged)
* preventing fields from being set
* more type overloading, conditionals on patterns and types, setting multiple fields from multiple words
* super-types and abstract types (underlying java of `supertypes` must be assignable from underying java of `type`)
* fields fetched by getters, written by setters
* passing arguments to constructors (besides the single maps)
 

## Serializers reference

We currently support the following manual serializers, in addition to many which are lower-level built-ins.
These can be set as annotations on the java class, or as serializers parsed and noted in the config,
either on a global or a per-class basis in the registry.

* `top-level-field` (`@YomlFieldAtTopLevel`)
  * means that a field is accepted at the top level (it does not need to be in a `field` block)
  
* `all-fields-top-level` (`@YomlAllFieldsAtTopLevel`)
  * applies the above to all fields

* `convert-singleton-map` (`@YomlSingletonMap`)
  * reads/writes an item as a single-key map where a field value is the key
  * particularly useful when working with a list of items to allow a concise multi-entry map syntax
  * defaults `.key` and `.value` facilitate working with `rename-...` serializers

* `config-map-constructor` (`@YomlConfigMapConstructor`)
  * indicates that config key static fields should be scanned and passed in a map to the constructor

* `convert-from-primitive` (`@YomlFromPrimitive`)
  * indicates that a primitive can be used for a complex object if just one non-trivial field is set


# TODO13 renames
# TODO13 bail out on collision rather than attempt to use "any value" in singleton map 
# TODO13 default-map-values
# TODO13 apply and test the above in a list as well, inferring type
# TODO13 convert-singleton-maps-in-list 
 


## Implementation notes

The `Yoml` entry point starts by invoking the `YamlConverter` which holds the input and 
output objects and instructions in a `YomlContext`, and runs through phases, applying  
`Serializer` instances on each phase.

Each `Serializer` exposes methods to `read`, `write`, and `document`, with the appropriate method 
invoked depending on what the `Converter` is doing.  A `Serializer` will typically check the phase 
and do nothing if it isn't appropriate; or if appropriate, they can:

* modify the objects in the context
* change the phases (restarting, ending, and/or inserting new ones to follow the current phase)
* add new serializers (e.g. once the type has been discovered, it may bring new serializers)

In addition, they can use a shared blackboard to store local information and communicate state.  
This loosely coupled mechanism gives a lot of flexibility for serializers do the right things in 
the right order whilst allowing them to be extended, but care does need to be taken.  
(The use of special phases and blackboards makes it easier to control what is done when.)

The general phases are:

* `manipulating` (custom serializers, operating directly on the input YAML map) 
* `handling-type` (default to instantaiate the java type, on read, or set the `type` field, on write),
  on read, sets the Java object and sets YamlKeysOnBlackboard which are subsequently used for manipulation;
  on write, sets the YAML object and sets JavaFieldsOnBlackboard (and sets ReadingTypeOnBlackboard with errors);
  inserting new phases:
  * when reading:
    * `manipulating` (custom serializers again, now with the object created, fields known, and other serializers loaded)
    * `handling-fields` (write the fields to the java object)
  * and when writing: 
    * `handling-fields` (collect the fields to write from the java object)
    * `manipulating` (custom serializers again, now with the type set and other serializers loaded)

Afterwards, a completion check runs across all blackboard items to confirm everything has been used
and to enable the most appropriate error to be returned to the user if there are any problems.




### TODO

* list/map conversion, and tidy up notes above
* rename-key
* complex syntax, type as key, etc

* infinite loop detection: in serialize loop
* handle references, solve infinite loop detection in self-referential writes, with `.reference: ../../OBJ`
* best-serialization vs first-serialization

* documentation
* yaml segment information and code-point completion



## Old notes

### Draft Use Case: An Init.d-style entity/effector language 

```
- id: print-all
  type: initdish-effector
  steps:
    00-provision: provision
    10-install:
      bash: |
        curl blah
        tar blah
    20-run:
      effector: 
        launch:
          parameters...
    21-run-other:
      type; invoke-effector
      effector: launch
      parameters:
        ...
```


### Alternate serialization approach (ignore)

(relying on sequencing and lots of defaults)

If the `serialization` field (which expects a list) is given a map, the `convert-singleton-map` 
serializer converts each <K,V> pair in that map to a list entry as follows:

* if V is a map, then the corresponding list entry is the map V with `{ .key: K }` added
* otherwise, the corresponding list entry is `{ .key: K, .value: V }`
 
Next, each entry in the list is interpreted as a `serialization` instance, 
and the serializations defined for that type specify:

* If the key `.value` is present and `type` is not defined, that key is renamed to `type` (ignored if `type` is already present)
  (code `rename-default-value: type`, handling `{ color: top-level-field }`)
* If the key `.key` is present and `.value` is not defined, that key is renamed to `.value`
* If it is a map of size exactly one, it is converted to a map with `convert-singleton-map` above, and phases not restarted
  (code `convert-singleton-maps-in-list`, handling `[ { top-level-field: color } ]`)
* If the key `.key` is present and `type` is not defined, that key is renamed to `type`
* If the item is a primitive V, it is converted to `{ .value: V }`, and phases not restarted
* If it is a map with no `type` defined, `type: top-level-field` is added

This allows the serialization rules defined on the specific type to kick in to handle `.key` or `.value` entries
introduced but not removed. In the case of `top-level-field` (the default type, as shown in the rules above), 
this will rename either such key `.value` to `field-name` (and give an error if `field-name` is already present). 

