from schematics.exceptions import ValidationError
from schematics.models import Model
from schematics.types import ModelType, StringType

from kp_scrapers.models.units import Unit
from kp_scrapers.models.validators import is_valid_cargo_movement, is_valid_numeric
from kp_scrapers.models.vessel import Player


class Cargo(Model):
    """Describe a cargo schema.

    REQUIRES AT LEAST ONE of the following fields:
        - product
        - buyer
        - seller

    Optional fields:
        - movement
        - volume
        - volume_unit
    """

    movement = StringType(metadata='build year of vessel', validators=[is_valid_cargo_movement])
    product = StringType(metadata='name of cargo')
    volume = StringType(metadata='absolute quantity of cargo', validators=[is_valid_numeric])
    volume_unit = StringType(
        metadata='unique numeric identifier of vessel', choices=[value for _, value in Unit]
    )

    buyer = ModelType(metadata='dict of buying player attributes', model_spec=Player)
    seller = ModelType(metadata='dict of selling player attributes', model_spec=Player)

    def validate_volume(self, model, volume):
        """Validate on a model-level if volume has an associated movement and unit.
        """
        # Note:  For now, we are suppressing the validation for movement
        # As per discussion with the PO, even though the source does not provide movement,
        # analyst might be able to fill the movement directly in the platform based on the context.

        # if volume and not model.get('movement'):
        #     raise ValidationError('Volume must have an associated `movement`')
        if volume and not model.get('volume_unit'):
            raise ValidationError('Volume must have an associated `volume_unit`')
        return volume
