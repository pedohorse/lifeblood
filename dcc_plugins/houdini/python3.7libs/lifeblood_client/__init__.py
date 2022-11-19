# this is a helper module that is SUPPOSED to be separate from lifeblood
# this module should be used by DCCs and other scripts that want to communicate with lifeblood
# but this module does NOT use lifeblood module directly, even if that means code duplication
# the point is to have some small light module without any dependencies
# that can be imported to a variety of DCCs and not cause any conflicts etc etc
