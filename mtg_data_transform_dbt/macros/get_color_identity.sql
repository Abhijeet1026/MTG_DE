{#
This macro returns the description of the color
#}

{% macro get_color_idenity (color_identity) -%}

-- https://scryfall.com/docs/api/colors

    case cast( {{color_identity}} as STRING)
        when "W" THEN "WHITE"
        when "B" THEN "BLACK"
        when "G" THEN "GREEN"
        when "U" THEN "BLUE"
        when "" THEN  "No_colour"
        else "Blended_Colors"

    end

{%- endmacro %}