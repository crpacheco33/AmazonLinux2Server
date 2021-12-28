from server.core.constants import Constants


def to_vendor_id(selling_partner_id):
    return selling_partner_id.split(
        Constants.PERIOD,
    )[-1]