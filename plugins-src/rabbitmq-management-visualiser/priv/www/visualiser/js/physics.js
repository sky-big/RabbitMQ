/*global octtree, vec3 */

function Newton() {
}
Newton.prototype.friction = 100;

Newton.prototype.update = function (elapsed, obj) {
    var incr;
    vec3.scale(obj.velocity, 1 - (this.friction * elapsed));
    incr = vec3.create(obj.velocity);
    vec3.scale(incr, elapsed);
    vec3.add(obj.pos, incr, obj.next_pos);
};

function Spring() {
}
Spring.prototype.k = 1;
Spring.prototype.equilibriumLength = 2;
Spring.prototype.push = true;
Spring.prototype.pull = true;
Spring.prototype.dampingFactor = 0.5;
Spring.prototype.octtreeRadius = 4;
Spring.prototype.octtreeLimit = 40;

Spring.prototype.apply = function (elapsed, obj1, obj2) {
    var damper, vecOP, distanceOP, x;
    damper = this.dampingFactor * elapsed * 100000;
    vecOP = vec3.create();
    distanceOP = 0;
    x = 0;
    vec3.subtract(obj2.pos, obj1.pos, vecOP);
    distanceOP = vec3.length(vecOP);
    if (!isNaN(distanceOP) && 0 !== distanceOP) {
        x = distanceOP - this.equilibriumLength;
        if (distanceOP > this.equilibriumLength && !this.pull) {
            return;
        }
        if (distanceOP < this.equilibriumLength && !this.push) {
            return;
        }
        vec3.scale(vecOP, (damper * (((1 / distanceOP) * x) / obj1.mass)));
        vec3.add(obj1.velocity, vecOP);
    }
};
Spring.prototype.update = function (elapsed, tree, obj) {
    var damper, vecOP, distanceOP, x, found, i, obj1;
    damper = this.dampingFactor * elapsed * 100000;
    vecOP = vec3.create();
    distanceOP = 0;
    x = 0;
    found = tree.findInRadius(obj.pos, this.octtreeRadius, this.octtreeLimit);
    for (i = 0; i < found.length; i += 1) {
        obj1 = found[i].value;
        if (obj1 !== obj) {
            // F = -k x where x is difference from equilibriumLength
            // a = F / m
            vec3.subtract(obj1.pos, obj.pos, vecOP);
            distanceOP = vec3.length(vecOP);
            if (!isNaN(distanceOP) && 0 !== distanceOP) {
                x = distanceOP - this.equilibriumLength;
                if (distanceOP > this.equilibriumLength && !this.pull) {
                    continue;
                }
                if (distanceOP < this.equilibriumLength && !this.push) {
                    continue;
                }
                vec3.scale(vecOP,
                           (damper * (((1 / distanceOP) * x) / obj.mass)));
                vec3.add(obj.velocity, vecOP);
            }
        }
    }
};

function Gravity() {
}
Gravity.prototype.bigG = 1 / 20;
Gravity.prototype.octtreeRadius = 5;
Gravity.prototype.octtreeLimit = 20;
Gravity.prototype.repel = false;

Gravity.prototype.update = function (elapsed, tree, obj) {
    var vecOP, distanceOP, found, i, obj1;
    vecOP = vec3.create();
    distanceOP = 0;
    found = tree.findInRadius(obj.pos, this.octtreeRadius, this.octtreeLimit);
    for (i = 0; i < found.length; i += 1) {
        obj1 = found[i].value;
        if (obj1 !== obj) {
            // F = G.m1.m2 / (d.d)
            // a = F / m1
            // thus a = G.m2/(d.d)
            vec3.subtract(obj1.pos, obj.pos, vecOP);
            distanceOP = vec3.length(vecOP);
            if ((!(isNaN(distanceOP))) && 0 !== distanceOP) {
                vec3.scale(vecOP, (this.bigG * obj1.mass) /
                           (distanceOP * distanceOP));
                if (this.repel) {
                    vec3.subtract(obj.velocity, vecOP);
                } else {
                    vec3.add(obj.velocity, vecOP);
                }
            }
        }
    }
};
