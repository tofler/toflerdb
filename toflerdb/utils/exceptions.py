
class ToflerDBException(Exception):
    '''super class of any exception raised by toflerdb'''


class RelationAlreadyIsA(ToflerDBException):
    '''return this when there is an attempt to set isA on a relation that is
    already isA something'''


class EntityAlreadyIsA(ToflerDBException):
    '''return this when there is an attempt to set isA on a entity that is
    already that thing'''


class EntityNotFound(ToflerDBException):
    '''return this when an entity was expected to exist in the database
    but didn't '''


class CouldNotSaveError(ToflerDBException):
    '''return this when there was a problem saving an object'''


class MissingRequiredFieldError(ToflerDBException):
    '''return this when any required field is missing'''


class InvalidInputValueError(ToflerDBException):
    '''return this when'''


class WriteLockedNodeError(ToflerDBException):
    '''return this when'''
