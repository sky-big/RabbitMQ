RabbitMQ Visualiser
===================


Usage
-----

This is a plugin for the RabbitMQ Management Plugin that provides an
HTML Canvas for rendering configured broker topology. The current main
purpose of this is for diagnostics and comprehension of the current
routing topology of the broker.

The left of the canvas displays exchanges, the right displays queues,
and the top displays channels. All of these items can be dragged
around the canvas. They repel one another, and snap back into their
predefined areas should they be released within the boundaries of those
areas.

Shift-clicking on an item hides it - it will be added to the relevant
select box on the left.

Hovering over an item shows at the top of the screen various details
about the item. Double-clicking on the item will take you to the
specific page in the Management Plugin concerning that item.

When hovering over an item, incoming links and/or traffic are shown in
green, whilst outgoing links and/or traffic are shown in
blue. Bindings are always displayed, but the consumers of a queue, and
likewise the publishers to an exchange, are only drawn in when
hovering over the exchange, queue or channel in question.

By default, up to 10 exchanges, 10 queues and 10 channels are
displayed. Additional resources are available from the left hand-side
select boxes, and can be brought into the display by selecting them
and clicking on the relevant 'Show' button.

The 'Display' check-boxes turn off and on entire resource classes, and
resets positioning.


Compatibility and Performance Notes
-----------------------------------

Does work in recent versions of Safari both on OS X and Windows.

Does work in Firefox (at least version 4.0).

Does work in Chrome.

Does not work in Internet Explorer. No error is given, but it doesn't
work.

Best performance is with Chrome. Note though that in some cases it has
been seen that hardware rendering (use of GPU) can actually slow down
performance. Some experimentation with browser flags and settings may
be necessary to ensure smooth operation.
