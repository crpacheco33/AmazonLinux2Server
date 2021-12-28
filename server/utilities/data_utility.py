from server.core.constants import Constants


class DataUtility:
    """Methods that are used in `server` to process data."""

    def delta(self, a, b):
        """Calculates the percentage difference between two numbers.

        The method interprets the situation when `b` is zero and `a` is
        greater than 0 to be a 100% increase. When both `a` and `b` are zero,
        the percentage difference is zero.

        Args:
            a: A number
            b: Another number

        Returns:
            `int` representing the percentage difference between `a` and `b`
        """
        try:
            difference = (
                (a - b) / b
            ) * 100
            return int(round(difference))
        except ZeroDivisionError:
            if a > 0:
                return 100
            else:
                return 0

    def to_camel_case(self, value):
        """Transforms snake case string into camel case.

        Args:
            value: `Snake-case text

        Returns:
            Camel-case text
        """
        lower_first_character = lambda s: s[:1].lower() + s[1:] if s else ''
        components = [word.title() for word in value.split(
            Constants.UNDERSCORE,
        )]
        
        if len(components) == 1: return value

        string = Constants.EMPTY_STRING.join(
            components,
        )
        return lower_first_character(string)

    def to_objectives_and_segments(self, items):
        """Groups items into objectives and segments.

        Objectives represent advertising objectives used by an agency and
        included the names of advertising campaigns and orders: audience modeling,
        brand protection, conquesting, depth in category, upsell,
        cross-merchandising, in-market, and re-market.
        
        Segments represent segments in the advertising funnel: awareness,
        consideration, and conversion.

        Args:
            items: Represents arbitrary objectives and segments
        
        Returns:
            A tuple of a list of objectives and a list of segments
        """
        all_objectives, all_segments = \
            ['AM', 'BP', 'CQ', 'DC', 'UZ', 'XM', 'CM', 'IM', 'RM'], \
            ['AW', 'CS', 'CV']
        
        objectives, segments = set(), set()

        if isinstance(items, str):
            items = items.split(Constants.COMMA)

        if items is None:
            return all_objectives, all_segments

        for item in items:
            if item in all_objectives:
                objectives.add(item)
            elif item in all_segments:
                segments.add(item)

                dsp_segments_as_objectives = Constants.FUNNEL_SEGMENT_AS_DSP_OBJECTIVES[item]
                sa_segments_as_objectives = Constants.FUNNEL_SEGMENT_AS_SA_OBJECTIVES[item]
                objectives.update(dsp_segments_as_objectives)
                objectives.update(sa_segments_as_objectives)
        
        return list(objectives), list(segments)

    def to_singular(self, value):
        """Transforms the plural version of an API model to its singular equivalent.
        
        This method is used with Amazon Advertising APIs presently.

        Args:
            value: Model name

        Returns:
            Singular version of model name
        """
        if value == 'line_item_creative_association': return value
        return value[:-1]