import re
from toml.encoder import _dump_str, TomlEncoder, InlineTableDict


class TomlFlatConfigEncoder(TomlEncoder):
    def __init__(self, _dict=dict):
        super(TomlFlatConfigEncoder, self).__init__(_dict)

    def dump_sections(self, o, sup):
        # print('in:', repr(o), repr(sup))

        retstr = ""
        if sup != "" and sup[-1] != ".":
            sup += '.'
        retdict = self._dict()
        arraystr = ""
        interesting_section = any(isinstance(x, self._dict) for x in o.values()) and any(not isinstance(x, self._dict) for x in o.values())
        for section in o:
            qsection = section
            if not re.match(r'^[A-Za-z0-9_-]+$', section):
                qsection = _dump_str(section)
            if not isinstance(o[section], dict):
                arrayoftables = False
                if isinstance(o[section], list):
                    for a in o[section]:
                        if isinstance(a, dict):
                            arrayoftables = True
                if arrayoftables:
                    for a in o[section]:
                        arraytabstr = "\n"
                        arraystr += "[[" + sup + qsection + "]]\n"
                        s, d = self.dump_sections(a, sup + qsection)
                        if s:
                            if s[0] == "[":
                                arraytabstr += s
                            else:
                                arraystr += s
                        while d:
                            newd = self._dict()
                            for dsec in d:
                                s1, d1 = self.dump_sections(d[dsec], sup +
                                                            qsection + "." +
                                                            dsec)
                                if s1:
                                    arraytabstr += ("[" + sup + qsection +
                                                    "." + dsec + "]\n")
                                    arraytabstr += s1
                                for s1 in d1:
                                    newd[dsec + "." + s1] = d1[s1]
                            d = newd
                        arraystr += arraytabstr
                else:
                    if o[section] is not None:
                        retstr += (qsection + " = " +
                                   str(self.dump_value(o[section])) + '\n')
            elif self.preserve and isinstance(o[section], InlineTableDict):
                retstr += (qsection + " = " +
                           self.dump_inline_table(o[section]))
            else:
                if interesting_section:
                    prefix = qsection
                    subrem = o[section]

                    def _helper(prefix, subrem):
                        nonlocal retstr
                        while subrem != {}:
                            subret, subrem = self.dump_sections(subrem, sup + prefix)
                            if subret:
                                retstr += '\n'.join(prefix + '.' + x for x in subret.strip().split('\n')) + '\n'
                            else:
                                # it seeeeems that in case subret == '' -
                                # then we can expect all keys of subrem to be already quoted....
                                # that's what dump_sections default implementation does
                                # so we don't need to quote subsection
                                for subsection in subrem:
                                    # qsubsection = subsection
                                    # if not re.match(r'^[A-Za-z0-9_-]+$', subsection):
                                    #     qsubsection = _dump_str(subsection)
                                    _helper(prefix + "." + subsection, subrem[subsection])
                                break
                    _helper(prefix, subrem)

                else:
                    retdict[qsection] = o[section]
        retstr += arraystr
        # print('out:', repr(retstr), repr(retdict))
        return retstr, retdict


if __name__ == '__main__':
    # not a test, but a demonstration
    import toml
    d = {'packages': {
        'houdini.py2': {
            '18.0.597': {
                'env': {
                    'PATH': {
                        'prepend': '/opt/hfs18.0.597/bin',
                        'append': 'ass'
                    }
                },
                'label': 'ass'
            },
            '18.5.408': {
                'env': {
                    'PATH': {
                        'prepend': '/opt/hfs18.5.408/bin'
                    },
                    'vata': [1, 2, 5]
                }
            }
        },
        'houdini.py3': {
            '18.5.499': {
                'env': {
                    'PATH': {
                        'prepend': '/opt/hfs18.5.499.py3/bin',
                        'append': 'me'
                    }
                }
            }
        },
            'bonker': {
                '18.5.499': {
                    'lobe.cabe': 2.3,
                    'iii': -1,
                    'env.shmenv': {
                        'PATH.SHMATH': {
                            'prepend': '/opt/hfs18.5.499.py3/bin',
                            'append': 'me'
                        },
                        'BOB.SHMOB': {
                            'prepend': 'you',
                            'append': 'me'
                        }
                    }
                }
            }
    }
    }
    r = toml.dumps(d, encoder=TomlFlatConfigEncoder())
    r2 = toml.dumps(d)
    print(r)
    print('\n\n============================\n\n')
    print(r2)
