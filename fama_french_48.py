import json 

with open('sic_to_ff_48.json', 'r') as f:
    SIC_TO_FF_48_DICT = {}
    for k, v in json.loads(f.read()).items():
        for subcategory, coderange in v['sic'].items():
            for sic in range(coderange['start'], coderange['end'] + 1):
                if sic in SIC_TO_FF_48_DICT.keys():
                    raise ValueError(
                        f'Duplicate SIC code {sic} in input file.'
                    )
                SIC_TO_FF_48_DICT[sic] = {
                    'code': k,
                    'name': v['name'],
                    'no': v['no'],
                    'subcategory': subcategory
                }
                
def sic_to_ff_48(sic_code):
    try:
        return SIC_TO_FF_48_DICT[sic_code]
    except KeyError as e:
        raise KeyError(
            f'SIC code not defined in Fama-French 48: {sic_code}'
        ) from e